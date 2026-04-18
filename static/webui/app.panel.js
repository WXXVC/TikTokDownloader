function resolveApiBase() {
  if (window.NEWWEB_API_BASE) {
    return window.NEWWEB_API_BASE;
  }
  if (window.location.protocol === "file:") {
    return "http://127.0.0.1:5555/panel-api";
  }
  return `${window.location.origin}/panel-api`;
}

const API_BASE = resolveApiBase();
const REQUEST_TIMEOUT_MS = 15000;
const SCAN_REQUEST_TIMEOUT_MS = 45000;
const POLL_INTERVAL_MS = 5000;
const POLL_BACKOFF_MS = 20000;
const MAX_CONSECUTIVE_POLL_FAILURES = 3;
const ACCESS_STORAGE_KEY = "newweb-access-granted";
const PROFILE_NAME_FORMAT_MODULES = [
  { key: "create_time", label: "发布时间", description: "作品发布时间" },
  { key: "type", label: "作品类型", description: "视频 / 图集等" },
  { key: "nickname", label: "账号昵称", description: "作者昵称" },
  { key: "desc", label: "作品描述", description: "作品标题或描述" },
  { key: "id", label: "作品 ID", description: "作品唯一 ID" },
  { key: "mark", label: "账号备注", description: "你设置的别名" },
  { key: "uid", label: "账号 ID", description: "账号 UID" },
];
const DEFAULT_PROFILE_NAME_FORMAT = ["create_time", "type", "nickname", "desc"];
const inFlightLoaders = new Set();
const loaderPromises = new Map();
const inFlightActions = new Set();
const activeRequests = new Map();
let backgroundPollHandle = null;
let pollingSuspendedUntil = 0;
let consecutivePollFailures = 0;
let lastPollWarningAt = 0;
let draggingProfileNameFormatIndex = null;

const state = {
  creators: [],
  creatorOptions: [],
  profiles: [],
  tasks: [],
  taskListLoaded: false,
  dashboardSummary: {
    auto_enabled_count: 0,
    auto_failed_count: 0,
    running_auto_tasks: 0,
    next_creator: null,
  },
  runningTaskCards: [],
  manualQueue: {
    count: 0,
    creator_ids: [],
    next_creator: null,
    path: "",
  },
  engineConfig: null,
  panelConfig: null,
  autoDownloadThrottle: null,
  riskGuardStatus: null,
  health: null,
  currentScan: null,
  creatorPage: {
    items: [],
    total: 0,
    page: 1,
    page_size: 100,
  },
  taskPage: {
    items: [],
    total: 0,
    page: 1,
    page_size: 100,
  },
  showDownloaded: false,
  selectedWorkIds: new Set(),
  viewingScanWorkId: null,
  editingCreatorId: null,
  viewingCreatorId: null,
  editingProfileId: null,
  viewingProfileId: null,
  retryingCreatorIds: new Set(),
  retryingFailedPage: false,
  creatorFilters: {
    keyword: "",
    platform: "",
    profileId: "",
    enabled: "",
    autoEnabled: "",
    autoStatus: "",
  },
  profileFilters: {
    keyword: "",
    enabled: "",
  },
  taskFilters: {
    keyword: "",
    status: "",
    mode: "",
    kind: "",
  },
  scanFilters: {
    keyword: "",
    type: "",
  },
  pagination: {
    creators: { page: 1, pageSize: 100 },
    profiles: { page: 1, pageSize: 50 },
    scan: { page: 1, pageSize: 50 },
    tasks: { page: 1, pageSize: 100 },
  },
  network: {
    pendingCount: 0,
    slowCount: 0,
    lastPath: "",
    lastError: "",
    pollingPausedUntil: 0,
  },
  digests: {
    profiles: "",
    creators: "",
    tasks: "",
    scan_cache: "",
  },
  snapshots: {
    health: "",
    throttle: "",
    risk: "",
    manualQueue: "",
    dashboard: "",
    runningTasks: "",
    profiles: "",
    creators: "",
    tasks: "",
  },
  profileNameFormatTokens: [...DEFAULT_PROFILE_NAME_FORMAT],
};

function renderNetworkStatus() {
  const bar = document.getElementById("network-status");
  const text = document.getElementById("network-status-text");
  if (!bar || !text) {
    return;
  }
  const { pendingCount, slowCount, lastPath, lastError, pollingPausedUntil } = state.network;
  const remainingPauseMs = Math.max(0, pollingPausedUntil - Date.now());
  bar.className = "network-status";
  if (!pendingCount && !slowCount && !lastError && !remainingPauseMs) {
    bar.classList.add("network-status-idle");
    text.textContent = "空闲";
    return;
  }
  if (lastError) {
    bar.classList.add("network-status-error");
  } else if (remainingPauseMs) {
    bar.classList.add("network-status-warn");
  } else if (slowCount) {
    bar.classList.add("network-status-warn");
  } else if (pendingCount) {
    bar.classList.add("network-status-busy");
  }
  const parts = [];
  if (pendingCount) {
    parts.push(`挂起请求 ${pendingCount}`);
  }
  if (slowCount) {
    parts.push(`慢请求 ${slowCount}`);
  }
  if (lastPath) {
    parts.push(`最近接口 ${lastPath}`);
  }
  if (remainingPauseMs) {
    parts.push(`轮询暂停 ${Math.ceil(remainingPauseMs / 1000)} 秒`);
  }
  if (lastError) {
    parts.push(`最近错误 ${lastError}`);
  }
  text.textContent = parts.join(" | ") || "空闲";
}

function suspendBackgroundPolling(reason) {
  pollingSuspendedUntil = Date.now() + POLL_BACKOFF_MS;
  state.network.pollingPausedUntil = pollingSuspendedUntil;
  renderNetworkStatus();
  const now = Date.now();
  if (now - lastPollWarningAt > 12000) {
    lastPollWarningAt = now;
    notify(`后台轮询已临时暂停 ${Math.ceil(POLL_BACKOFF_MS / 1000)} 秒：${reason}`, "warning");
  }
}

function beginTrackedRequest(path) {
  const requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const startedAt = Date.now();
  const slowTimer = window.setTimeout(() => {
    const entry = activeRequests.get(requestId);
    if (!entry || entry.slow) {
      return;
    }
    entry.slow = true;
    state.network.slowCount += 1;
    state.network.lastPath = path;
    renderNetworkStatus();
  }, 1200);
  activeRequests.set(requestId, {
    path,
    startedAt,
    slow: false,
    slowTimer,
  });
  state.network.pendingCount = activeRequests.size;
  state.network.lastPath = path;
  renderNetworkStatus();
  return requestId;
}

function finishTrackedRequest(requestId, errorMessage = "") {
  const entry = activeRequests.get(requestId);
  if (!entry) {
    return;
  }
  window.clearTimeout(entry.slowTimer);
  if (entry.slow) {
    state.network.slowCount = Math.max(0, state.network.slowCount - 1);
  }
  activeRequests.delete(requestId);
  state.network.pendingCount = activeRequests.size;
  state.network.lastPath = entry.path;
  state.network.lastError = errorMessage;
  renderNetworkStatus();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function notify(message, type = "info", actions = []) {
  const container = document.getElementById("toast-container");
  if (!container) {
    window.alert(message);
    return;
  }
  const toast = document.createElement("article");
  toast.className = `toast toast-${type}`;
  const body = document.createElement("div");
  body.className = "toast-body";
  body.innerHTML = escapeHtml(message).replaceAll("\n", "<br />");

  const closeButton = document.createElement("button");
  closeButton.type = "button";
  closeButton.className = "toast-close";
  closeButton.setAttribute("aria-label", "关闭提示");
  closeButton.textContent = "关闭";

  toast.append(body, closeButton);

  if (actions.length) {
    const actionsRow = document.createElement("div");
    actionsRow.className = "toast-actions";
    actions.forEach((action) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `toast-action-button${action.kind === "danger" ? " toast-action-danger" : ""}`;
      button.textContent = action.label;
      button.addEventListener("click", () => {
        action.onClick?.();
        removeToast();
      });
      actionsRow.appendChild(button);
    });
    body.appendChild(actionsRow);
  }

  const removeToast = () => {
    toast.classList.add("toast-leaving");
    window.setTimeout(() => toast.remove(), 180);
  };
  closeButton.addEventListener("click", removeToast);
  container.prepend(toast);
  window.setTimeout(removeToast, type === "error" ? 7000 : 4200);
}

function formatError(error) {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function parseJsonField(value, label) {
  const text = String(value || "").trim();
  if (!text) {
    return {};
  }
  try {
    const data = JSON.parse(text);
    if (!data || typeof data !== "object" || Array.isArray(data)) {
      throw new Error(`${label} 必须是 JSON 对象。`);
    }
    return data;
  } catch (error) {
    throw new Error(`${label} JSON 格式错误：${formatError(error)}`);
  }
}

function stableStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (error) {
    return String(value);
  }
}

function buildHealthSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    status: data.status,
    app_db_ready: data.app_db_ready,
    engine_db_found: data.engine_db_found,
    engine_settings_found: data.engine_settings_found,
  });
}

function buildManualQueueSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    count: data.count ?? 0,
    creator_ids: Array.isArray(data.creator_ids) ? data.creator_ids : [],
    next_creator: data.next_creator
      ? {
        id: data.next_creator.id,
        mark: data.next_creator.mark,
        name: data.next_creator.name,
        platform: data.next_creator.platform,
      }
      : null,
  });
}

function buildThrottleSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    is_paused: data.is_paused,
    remaining_seconds: data.remaining_seconds,
    works_count: data.works_count,
    creators_count: data.creators_count,
    works_window_minutes: data.works_window_minutes,
    last_reason: data.last_reason,
  });
}

function buildRiskSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    is_active: data.is_active,
    remaining_seconds: data.remaining_seconds,
    last_reason: data.last_reason,
    last_triggered_at: data.last_triggered_at,
    http_error_streak: data.http_error_streak,
    empty_download_streak: data.empty_download_streak,
    low_quality_streak: data.low_quality_streak,
  });
}

function buildDashboardSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    auto_enabled_count: data.auto_enabled_count ?? 0,
    auto_failed_count: data.auto_failed_count ?? 0,
    running_auto_tasks: data.running_auto_tasks ?? 0,
    next_creator: data.next_creator
      ? {
        id: data.next_creator.id,
        mark: data.next_creator.mark,
        name: data.next_creator.name,
        auto_download_next_run_at: data.next_creator.auto_download_next_run_at,
      }
      : null,
  });
}

function buildRunningTasksSnapshot(data) {
  if (!Array.isArray(data)) {
    return "";
  }
  return stableStringify(
    data.map((item) => ({
      task_id: item.task_id,
      creator_id: item.creator_id,
      creator_name: item.creator_name,
      mode: item.mode,
      total_count: item.total_count,
      completed_count: item.completed_count,
      progress_percent: item.progress_percent,
      target_folder_name: item.target_folder_name,
      message: item.message,
    })),
  );
}

function buildProfilesSnapshot(items) {
  return stableStringify((items || []).map((item) => ({
    id: item.id,
    name: item.name,
    enabled: item.enabled,
    updated_at: item.updated_at,
    root_path: item.root_path,
    folder_name: item.folder_name,
    name_format: item.name_format,
    folder_mode: item.folder_mode,
    music: item.music,
    dynamic_cover: item.dynamic_cover,
    static_cover: item.static_cover,
  })));
}

function buildCreatorsSnapshot(items) {
  const pageData = Array.isArray(items)
    ? { items, total: items.length, page: 1, page_size: items.length }
    : (items || { items: [], total: 0, page: 1, page_size: 100 });
  return stableStringify({
    total: pageData.total || 0,
    page: pageData.page || 1,
    page_size: pageData.page_size || 100,
    items: (pageData.items || []).map((item) => ({
      id: item.id,
      name: item.name,
      mark: item.mark,
      enabled: item.enabled,
      profile_id: item.profile_id,
      updated_at: item.updated_at,
      auto_download_enabled: item.auto_download_enabled,
      auto_download_interval_minutes: item.auto_download_interval_minutes,
      auto_download_next_run_at: item.auto_download_next_run_at,
      auto_download_last_status: item.auto_download_last_status,
      auto_download_last_message: item.auto_download_last_message,
    })),
  });
}

function buildTasksSnapshot(data) {
  const pageData = Array.isArray(data)
    ? { items: data, total: data.length, page: 1, page_size: data.length }
    : (data || { items: [], total: 0, page: 1, page_size: 100 });
  return stableStringify({
    total: pageData.total || 0,
    page: pageData.page || 1,
    page_size: pageData.page_size || 100,
    items: (pageData.items || []).map((item) => ({
      creator_id: item.creator_id,
      creator_name: item.creator_name,
      platform: item.platform,
      mark: item.mark,
      video_count: item.video_count,
      collection_count: item.collection_count,
      failed_count: item.failed_count,
      last_download_at: item.last_download_at,
    })),
  });
}

function buildDigestSnapshot(data) {
  if (!data) {
    return "";
  }
  return stableStringify({
    count: data.count ?? 0,
    updated_at: data.updated_at || "",
  });
}

async function runAction(action) {
  try {
    await action();
  } catch (error) {
    console.error(error);
    state.network.lastError = formatError(error);
    renderNetworkStatus();
    notify(formatError(error), "error");
  }
}

async function runLockedAction(key, action) {
  if (inFlightActions.has(key)) {
    return false;
  }
  inFlightActions.add(key);
  try {
    await runAction(action);
    return true;
  } finally {
    inFlightActions.delete(key);
  }
}

async function request(path, options = {}) {
  const requestId = beginTrackedRequest(path);
  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : REQUEST_TIMEOUT_MS;
  const timeoutHandle = window.setTimeout(() => controller.abort(), timeoutMs);
  const method = String(options.method || "GET").toUpperCase();
  const requestPath = method === "GET" || method === "HEAD"
    ? `${path}${path.includes("?") ? "&" : "?"}_=${Date.now()}`
    : path;
  let response;
  try {
    response = await fetch(`${API_BASE}${requestPath}`, {
      headers: {
        "Content-Type": "application/json",
        ...(method === "GET" || method === "HEAD"
          ? {
              "Cache-Control": "no-cache, no-store, must-revalidate",
              Pragma: "no-cache",
            }
          : {}),
        ...(options.headers || {}),
      },
      cache: method === "GET" || method === "HEAD" ? "no-store" : options.cache,
      ...options,
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(`请求超时：${path}`);
      finishTrackedRequest(requestId, timeoutError.message);
      throw timeoutError;
    }
    finishTrackedRequest(requestId, error?.message || String(error));
    throw error;
  }

  try {
    if (!response.ok) {
      const text = await response.text();
      const responseError = new Error(text || "Request failed");
      finishTrackedRequest(requestId, responseError.message);
      throw responseError;
    }

    if (response.status === 204) {
      finishTrackedRequest(requestId, "");
      return null;
    }

    const data = await response.json();
    finishTrackedRequest(requestId, "");
    return data;
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(`响应读取超时：${path}`);
      finishTrackedRequest(requestId, timeoutError.message);
      throw timeoutError;
    }
    finishTrackedRequest(requestId, error?.message || String(error));
    throw error;
  } finally {
    window.clearTimeout(timeoutHandle);
  }
}

async function withLoaderLock(key, action) {
  if (loaderPromises.has(key)) {
    return loaderPromises.get(key);
  }
  const promise = (async () => {
    inFlightLoaders.add(key);
    try {
      return await action();
    } finally {
      inFlightLoaders.delete(key);
      loaderPromises.delete(key);
    }
  })();
  loaderPromises.set(key, promise);
  return promise;
}

function boolValue(form, name) {
  return form.querySelector(`[name="${name}"]`).checked;
}

function getCreatorForm() {
  return document.getElementById("creator-form");
}

function getProfileForm() {
  return document.getElementById("profile-form");
}

function getEngineConfigForm() {
  return document.getElementById("engine-config-form");
}

function getCreatorModal() {
  return document.getElementById("creator-modal");
}

function getCreatorDetailModal() {
  return document.getElementById("creator-detail-modal");
}

function getProfileModal() {
  return document.getElementById("profile-modal");
}

function getProfileDetailModal() {
  return document.getElementById("profile-detail-modal");
}

function getScanDetailModal() {
  return document.getElementById("scan-detail-modal");
}

function creatorDisplayName(item) {
  return item.mark || item.name;
}

function profileNameById(profileId) {
  return state.profiles.find((item) => item.id === Number(profileId))?.name || `配置 ${profileId ?? 1}`;
}

function formatIntervalLabel(minutes) {
  const value = Number(minutes || 0);
  if (!value) {
    return "未开启";
  }
  if (value % 1440 === 0) {
    return `每 ${value / 1440} 天`;
  }
  if (value % 60 === 0) {
    return `每 ${value / 60} 小时`;
  }
  return `每 ${value} 分钟`;
}

function getAutoResultMeta(status) {
  switch (String(status || "").toLowerCase()) {
    case "queued":
      return { label: "排队中", pillClass: "pill-info", cardClass: "auto-status-card-progress" };
    case "scanning":
      return { label: "扫描中", pillClass: "pill-info", cardClass: "auto-status-card-progress" };
    case "queueing":
      return { label: "入队中", pillClass: "pill-info", cardClass: "auto-status-card-progress" };
    case "running":
      return { label: "下载中", pillClass: "pill-accent", cardClass: "auto-status-card-active" };
    case "scheduled":
      return { label: "已入队", pillClass: "pill-accent", cardClass: "auto-status-card-active" };
    case "success":
      return { label: "最近成功", pillClass: "pill-success", cardClass: "auto-status-card-active" };
    case "failed":
      return { label: "最近失败", pillClass: "pill-warn", cardClass: "auto-status-card-failed" };
    case "stopped":
      return { label: "已停止", pillClass: "pill-muted", cardClass: "auto-status-card-muted" };
    case "idle":
      return { label: "无新作品", pillClass: "pill-muted", cardClass: "auto-status-card-muted" };
    case "skipped":
      return { label: "已跳过", pillClass: "pill-muted", cardClass: "auto-status-card-muted" };
    default:
      return { label: "暂无记录", pillClass: "pill-muted", cardClass: "auto-status-card-muted" };
  }
}

function getCreatorLiveAutoState(item) {
  const runningTask = (state.runningTaskCards || []).find((task) => Number(task.creator_id) === Number(item.id));
  if (runningTask) {
    return {
      status: "running",
      message: runningTask.message || item.auto_download_last_message || "",
    };
  }
  return {
    status: item.auto_download_last_status,
    message: item.auto_download_last_message || "",
  };
}

function formatCreatorTabLabel(tab) {
  switch ((tab || "post").toLowerCase()) {
    case "favorite":
      return "喜欢作品";
    case "collection":
      return "收藏作品";
    case "post":
    default:
      return "发布作品";
  }
}

function parseProfileNameFormat(value) {
  const tokens = String(value || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .filter((token, index, list) => list.indexOf(token) === index)
    .filter((token) => PROFILE_NAME_FORMAT_MODULES.some((item) => item.key === token));
  return tokens.length ? tokens : [...DEFAULT_PROFILE_NAME_FORMAT];
}

function serializeProfileNameFormat(tokens = state.profileNameFormatTokens) {
  return (Array.isArray(tokens) && tokens.length ? tokens : DEFAULT_PROFILE_NAME_FORMAT).join(" ");
}

function getProfileNameFormatModule(key) {
  return PROFILE_NAME_FORMAT_MODULES.find((item) => item.key === key) || { key, label: key, description: "" };
}

function syncProfileNameFormatInput() {
  const form = getProfileForm();
  if (form?.name_format) {
    form.name_format.value = serializeProfileNameFormat();
  }
}

function renderProfileNameFormatBuilder() {
  const list = document.getElementById("profile-name-format-list");
  const picker = document.getElementById("profile-name-format-picker");
  const plusButton = document.getElementById("profile-name-format-plus");
  if (!list || !picker || !plusButton) {
    return;
  }
  const tokens = Array.isArray(state.profileNameFormatTokens) && state.profileNameFormatTokens.length
    ? state.profileNameFormatTokens
    : [...DEFAULT_PROFILE_NAME_FORMAT];
  list.innerHTML = tokens.length
    ? tokens.map((token, index) => {
      const moduleItem = getProfileNameFormatModule(token);
      return `
        <div class="name-format-chip" data-name-format-key="${moduleItem.key}" data-name-format-index="${index}" draggable="true">
          <div>
            <strong>${escapeHtml(moduleItem.label)}</strong>
            <small>${escapeHtml(moduleItem.description)}</small>
          </div>
          <div class="name-format-chip-actions">
            <button type="button" title="删除" aria-label="删除" data-name-format-remove="${index}" ${tokens.length === 1 ? "disabled" : ""}>×</button>
          </div>
        </div>
      `;
    }).join("")
    : `<div class="name-format-empty">请至少保留一个命名模块。</div>`;
  picker.innerHTML = [
    ...PROFILE_NAME_FORMAT_MODULES
      .filter((item) => !tokens.includes(item.key))
      .map((item) => `<button type="button" data-name-format-add="${item.key}"><strong>${escapeHtml(item.label)}</strong><small>${escapeHtml(item.description)} (${escapeHtml(item.key)})</small></button>`),
  ].join("");
  plusButton.hidden = PROFILE_NAME_FORMAT_MODULES.every((item) => tokens.includes(item.key));
  if (!picker.innerHTML.trim()) {
    picker.innerHTML = `<div class="name-format-empty">没有可添加的模块了。</div>`;
  }
  syncProfileNameFormatInput();
}

function setProfileNameFormat(tokens) {
  state.profileNameFormatTokens = parseProfileNameFormat(Array.isArray(tokens) ? tokens.join(" ") : tokens);
  renderProfileNameFormatBuilder();
}

function moveProfileNameFormatTokenToIndex(fromIndex, toIndex) {
  const tokens = [...state.profileNameFormatTokens];
  if (
    fromIndex == null ||
    toIndex == null ||
    fromIndex < 0 ||
    toIndex < 0 ||
    fromIndex >= tokens.length ||
    toIndex >= tokens.length ||
    fromIndex === toIndex
  ) {
    return;
  }
  const [moved] = tokens.splice(fromIndex, 1);
  tokens.splice(toIndex, 0, moved);
  state.profileNameFormatTokens = tokens;
  renderProfileNameFormatBuilder();
}

function removeProfileNameFormatToken(index) {
  const tokens = state.profileNameFormatTokens.filter((_, tokenIndex) => tokenIndex !== index);
  state.profileNameFormatTokens = tokens.length ? tokens : [DEFAULT_PROFILE_NAME_FORMAT[0]];
  renderProfileNameFormatBuilder();
}

function addProfileNameFormatToken(token) {
  if (!token || state.profileNameFormatTokens.includes(token)) {
    return;
  }
  state.profileNameFormatTokens = [...state.profileNameFormatTokens, token];
  renderProfileNameFormatBuilder();
}

function openProfileNameFormatPicker() {
  const picker = document.getElementById("profile-name-format-picker");
  const plusButton = document.getElementById("profile-name-format-plus");
  if (!picker || !plusButton || plusButton.hidden) {
    return;
  }
  picker.hidden = false;
  plusButton.setAttribute("aria-expanded", "true");
}

function closeProfileNameFormatPicker() {
  const picker = document.getElementById("profile-name-format-picker");
  const plusButton = document.getElementById("profile-name-format-plus");
  if (!picker || !plusButton) {
    return;
  }
  picker.hidden = true;
  plusButton.setAttribute("aria-expanded", "false");
}

function splitIntervalMinutes(totalMinutes) {
  const value = Math.max(0, Number(totalMinutes || 0));
  if (!value) {
    return { value: 0, unit: "m" };
  }
  if (value % 1440 === 0) {
    return { value: value / 1440, unit: "d" };
  }
  if (value % 60 === 0) {
    return { value: value / 60, unit: "h" };
  }
  return { value, unit: "m" };
}

function joinIntervalMinutes(value, unit) {
  const amount = Math.max(0, Number(value || 0));
  if (!amount) {
    return 0;
  }
  if (unit === "d") {
    return amount * 1440;
  }
  if (unit === "h") {
    return amount * 60;
  }
  return amount;
}

function formatTaskModeLabel(mode) {
  switch (mode) {
    case "creator_batch_download":
      return "手动整号下载";
    case "detail_download":
      return "手动作品下载";
    case "auto_detail_download":
      return "自动扫描下载";
    case "auto_creator_batch_download":
      return "自动整号下载";
    default:
      return mode || "(未知模式)";
  }
}

function isAutoTask(mode) {
  return mode === "auto_detail_download" || mode === "auto_creator_batch_download";
}

function switchView(viewId) {
  document.querySelectorAll(".nav-link").forEach((item) => item.classList.toggle("active", item.dataset.view === viewId));
  document.querySelectorAll(".view").forEach((item) => item.classList.toggle("active", item.id === viewId));
  if (viewId === "tasks") {
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  }
}

function focusCreatorsWithFilters(filters = {}) {
  state.creatorFilters = {
    ...state.creatorFilters,
    ...filters,
  };
  state.pagination.creators.page = 1;
  switchView("creators");
  loadCreators().catch((error) => {
    console.error(error);
    notify(formatError(error), "error");
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function focusTasksWithFilters(filters = {}) {
  state.taskFilters = {
    ...state.taskFilters,
    ...filters,
  };
  state.pagination.tasks.page = 1;
  switchView("tasks");
  withLoaderLock("load:tasks", loadTasks).catch((error) => {
    console.error(error);
    notify(formatError(error), "error");
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function openCreatorDetailFromDashboard(creatorId) {
  request(`/creators/${creatorId}`)
    .then((item) => openCreatorDetailModal(item))
    .catch((error) => {
      console.error(error);
      notify(`没有找到该账号的详情：${formatError(error)}`, "error");
    });
}

function renderHealth(data = state.health) {
  if (!data) {
    return;
  }

  const dashboard = state.dashboardSummary || {};
  const autoCreatorsCount = Number(dashboard.auto_enabled_count || 0);
  const failedAutoCreatorsCount = Number(dashboard.auto_failed_count || 0);
  const runningAutoTasksCount = Number(dashboard.running_auto_tasks || 0);
  const autoSchedulerEnabled = Boolean(state.panelConfig?.auto_download_scheduler_enabled);
  const manualQueue = state.manualQueue || {};
  const manualQueueCount = Number(manualQueue.count || 0);
  const manualQueueNextCreator = manualQueue.next_creator || null;
  const throttle = state.autoDownloadThrottle;
  const riskGuard = state.riskGuardStatus;
  const pauseMode = state.panelConfig?.auto_download_pause_mode === "creators" ? "creators" : "works";
  const throttleSummary = throttle?.is_paused
    ? `暂停中，剩余 ${Math.ceil((throttle.remaining_seconds || 0) / 60)} 分钟`
    : pauseMode === "creators"
      ? `正常，最近 ${throttle?.works_window_minutes || state.panelConfig?.auto_download_pause_window_minutes || 30} 分钟已处理 ${throttle?.creators_count || 0} 个账号`
      : `正常，最近 ${throttle?.works_window_minutes || state.panelConfig?.auto_download_pause_window_minutes || 30} 分钟已成功下载 ${throttle?.works_count || 0} 个作品`;
  const riskSummary = riskGuard?.is_active
    ? `冷却中，剩余 ${Math.ceil((riskGuard.remaining_seconds || 0) / 3600)} 小时`
    : `正常，HTTP ${riskGuard?.http_error_streak || 0} / 空地址 ${riskGuard?.empty_download_streak || 0} / 低清晰 ${riskGuard?.low_quality_streak || 0}`;
  const riskDetail = riskGuard?.last_reason
    ? `${riskGuard.last_reason}${riskGuard.last_triggered_at ? ` 最近触发：${riskGuard.last_triggered_at}` : ""}`
    : "当前没有风控触发记录。";
  const nextCreator = dashboard.next_creator || null;
  const runningTaskCards = Array.isArray(state.runningTaskCards) ? state.runningTaskCards : [];
  const runningTaskMarkup = runningTaskCards.length
    ? runningTaskCards.map((item) => {
      const totalCount = Math.max(0, Number(item.total_count || 0));
      const completedCount = Math.max(0, Number(item.completed_count || 0));
      const progressPercent = Math.max(0, Math.min(100, Number(item.progress_percent || 0)));
      return `
      <article class="running-task-card" role="button" tabindex="0" data-running-task-creator="${item.creator_id}">
        <header>
          <div>
            <strong>${escapeHtml(item.creator_name || `账号 ${item.creator_id}`)}</strong>
            <small>${escapeHtml(item.platform || "")}${item.mode === "auto_detail_download" ? " 自动下载" : item.mode === "auto_creator_batch_download" ? " 自动整号下载" : item.mode === "detail_download" ? " 手动作业" : item.mode === "creator_batch_download" ? " 整号下载" : ""}</small>
          </div>
          <span class="pill pill-accent">执行中</span>
        </header>
        <p>${escapeHtml(item.target_folder_name || item.message || "任务执行中")}</p>
        <div class="running-task-progress">
          <div class="running-task-progress-meta">
            <strong>已完成 ${completedCount} / ${totalCount || "?"}</strong>
            <small>${progressPercent}%</small>
          </div>
          <div class="running-task-progress-bar" aria-hidden="true">
            <span style="width:${progressPercent}%"></span>
          </div>
        </div>
      </article>
    `;
    }).join("")
    : `<div class="stat-card stat-card-wide"><strong>(暂无)</strong><small>当前没有执行中的下载任务</small></div>`;

  document.getElementById("health-grid").innerHTML = `
    <div class="stat-card"><strong>${escapeHtml(data.status)}</strong><small>接口状态</small></div>
    <div class="stat-card"><strong>${data.app_db_ready ? "已就绪" : "缺失"}</strong><small>面板存储</small></div>
    <div class="stat-card"><strong>${data.engine_db_found ? "已发现" : "缺失"}</strong><small>引擎下载历史库</small></div>
    <div class="stat-card"><strong>${data.engine_settings_found ? "已发现" : "缺失"}</strong><small>引擎 settings.json</small></div>
    <div class="stat-card ${autoSchedulerEnabled ? "" : "stat-card-warn"}"><strong>${autoSchedulerEnabled ? "已开启" : "已关闭"}</strong><small>全局自动下载调度</small></div>
    <div class="stat-card ${manualQueueCount ? "stat-card-warn" : ""}"><strong>${escapeHtml(manualQueueCount)}</strong><small>当前手动队列数量</small></div>
    ${manualQueueNextCreator
      ? `<button type="button" class="stat-card stat-card-button" data-dashboard-action="manual-queue-next" data-creator-id="${manualQueueNextCreator.id}"><strong>${escapeHtml(creatorDisplayName(manualQueueNextCreator))}</strong><small>手动队列中的下一个账号</small></button>`
      : `<div class="stat-card"><strong>(暂无)</strong><small>手动队列中的下一个账号</small></div>`}
    <button type="button" class="stat-card stat-card-button stat-card-accent" data-dashboard-action="auto-creators"><strong>${escapeHtml(autoCreatorsCount)}</strong><small>已开启自动下载的账号</small></button>
    <button type="button" class="stat-card stat-card-button" data-dashboard-action="running-auto-tasks"><strong>${escapeHtml(runningAutoTasksCount)}</strong><small>运行中的自动任务</small></button>
    <button type="button" class="stat-card stat-card-button ${failedAutoCreatorsCount ? "stat-card-warn" : ""}" data-dashboard-action="failed-creators"><strong>${escapeHtml(failedAutoCreatorsCount)}</strong><small>最近执行失败的账号</small></button>
    <div class="stat-card ${throttle?.is_paused ? "stat-card-warn" : ""}"><strong>${throttle?.is_paused ? "已暂停" : "正常"}</strong><small>${escapeHtml(throttleSummary)}</small></div>
    <div class="stat-card ${riskGuard?.is_active ? "stat-card-warn" : ""}">
      <strong>${riskGuard?.is_active ? "风控冷却中" : "风控正常"}</strong>
      <small>${escapeHtml(riskSummary)}</small>
      <small>${escapeHtml(riskDetail)}</small>
      ${riskGuard?.is_active ? '<button type="button" class="ghost-button stat-inline-button" data-reset-risk-guard="true">解除冷却</button>' : ""}
    </div>
    <div class="stat-card stat-card-wide running-task-section">
      <strong>当前执行任务</strong>
      <small>这里只展示正在下载中的账号任务，完成后会自动消失。</small>
      <div class="running-task-grid">${runningTaskMarkup}</div>
    </div>
    ${nextCreator
      ? `<button type="button" class="stat-card stat-card-button stat-card-wide" data-dashboard-action="next-creator" data-creator-id="${nextCreator.id}"><strong>${escapeHtml(creatorDisplayName(nextCreator))}</strong><small>${escapeHtml(`最近待执行：${nextCreator.auto_download_next_run_at}`)}</small></button>`
      : `<div class="stat-card stat-card-wide"><strong>(暂无)</strong><small>最近待执行账号</small></div>`}
  `;

  document.querySelectorAll("[data-dashboard-action='auto-creators']").forEach((button) => {
    button.addEventListener("click", () => {
      focusCreatorsWithFilters({
        autoEnabled: "true",
        autoStatus: "",
      });
    });
  });

  document.querySelectorAll("[data-dashboard-action='running-auto-tasks']").forEach((button) => {
    button.addEventListener("click", () => {
      focusTasksWithFilters({
        kind: "auto",
        status: "running",
      });
    });
  });

  document.querySelectorAll("[data-dashboard-action='failed-creators']").forEach((button) => {
    button.addEventListener("click", () => {
      focusCreatorsWithFilters({
        autoEnabled: "true",
        autoStatus: "failed",
      });
    });
  });

  document.querySelectorAll("[data-dashboard-action='manual-queue-next']").forEach((button) => {
    button.addEventListener("click", () => {
      openCreatorDetailFromDashboard(button.dataset.creatorId);
    });
  });

  document.querySelectorAll("[data-dashboard-action='next-creator']").forEach((button) => {
    button.addEventListener("click", () => {
      openCreatorDetailFromDashboard(button.dataset.creatorId);
    });
  });

  document.querySelectorAll("[data-running-task-creator]").forEach((card) => {
    const openDetail = () => openCreatorDetailFromDashboard(card.dataset.runningTaskCreator);
    card.addEventListener("click", openDetail);
    card.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      openDetail();
    });
  });

  document.querySelectorAll("[data-reset-risk-guard]").forEach((button) => {
    button.addEventListener("click", () => runAction(async () => {
      await request("/panel/risk-guard/reset", { method: "POST" });
      await loadRiskGuardStatus();
      await loadCreators();
      await withLoaderLock("load:tasks", loadTasks);
      await loadHealth();
      notify("已手动解除风控冷却。", "success");
    }));
  });
}

function toDatetimeLocalValue(value) {
  if (!value) {
    return "";
  }
  return String(value).slice(0, 16);
}

function filteredProfiles() {
  return state.profiles.filter((item) => {
    const keyword = state.profileFilters.keyword.trim().toLowerCase();
    if (keyword) {
      const haystack = [
        item.name,
        item.root_path,
        item.folder_name,
        item.name_format,
      ].join(" ").toLowerCase();
      if (!haystack.includes(keyword)) {
        return false;
      }
    }

    if (state.profileFilters.enabled) {
      const expected = state.profileFilters.enabled === "true";
      if (Boolean(item.enabled) !== expected) {
        return false;
      }
    }

    return true;
  });
}

function filteredTasks() {
  return state.tasks.filter((item) => {
    const keyword = state.taskFilters.keyword.trim().toLowerCase();
    if (keyword) {
      const haystack = [
        item.creator_name,
        item.platform,
        item.run_command,
        item.message,
      ].join(" ").toLowerCase();
      if (!haystack.includes(keyword)) {
        return false;
      }
    }

    if (state.taskFilters.status && item.status !== state.taskFilters.status) {
      return false;
    }

    if (state.taskFilters.mode && item.mode !== state.taskFilters.mode) {
      return false;
    }

    if (state.taskFilters.kind === "auto" && !isAutoTask(item.mode)) {
      return false;
    }

    if (state.taskFilters.kind === "manual" && isAutoTask(item.mode)) {
      return false;
    }

    return true;
  });
}

function currentScanPageItems() {
  return state.currentScan?.items || [];
}

function currentCreatorPageItems() {
  return state.creatorPage.items || [];
}

function ensureValidPage(key, totalItems) {
  const meta = state.pagination[key];
  const totalPages = Math.max(1, Math.ceil(totalItems / meta.pageSize));
  if (meta.page > totalPages) {
    meta.page = totalPages;
  }
  if (meta.page < 1) {
    meta.page = 1;
  }
  return totalPages;
}

function paginateItems(key, items) {
  const meta = state.pagination[key];
  const totalPages = ensureValidPage(key, items.length);
  const start = (meta.page - 1) * meta.pageSize;
  const end = start + meta.pageSize;
  return {
    items: items.slice(start, end),
    totalItems: items.length,
    totalPages,
    page: meta.page,
    pageSize: meta.pageSize,
    start: items.length ? start + 1 : 0,
    end: Math.min(end, items.length),
  };
}

function setPage(key, page) {
  state.pagination[key].page = page;
}

function setPageSize(key, pageSize) {
  state.pagination[key].pageSize = pageSize;
  state.pagination[key].page = 1;
}

function renderPagination(targetId, key, pageData, rerender) {
  const container = document.getElementById(targetId);
  if (!container) {
    return;
  }

  container.innerHTML = `
    <span class="pagination-info">
      第 ${pageData.page} / ${pageData.totalPages} 页，显示 ${pageData.start}-${pageData.end} / ${pageData.totalItems}
    </span>
    <button type="button" data-page-action="prev" ${pageData.page <= 1 ? "disabled" : ""}>上一页</button>
    <button type="button" data-page-action="next" ${pageData.page >= pageData.totalPages ? "disabled" : ""}>下一页</button>
  `;

  container.querySelectorAll("[data-page-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextPage = button.dataset.pageAction === "prev" ? pageData.page - 1 : pageData.page + 1;
      setPage(key, nextPage);
      rerender();
    });
  });
}

function updateFormModeText() {
  document.getElementById("creator-submit").textContent = state.editingCreatorId
    ? "保存账号"
    : "新增账号";
  document.getElementById("profile-submit").textContent = state.editingProfileId
    ? "保存配置"
    : "新增配置";
  document.getElementById("creator-cancel").style.display = state.editingCreatorId
    ? "inline-flex"
    : "none";
  document.getElementById("creator-test-auto").style.display = state.editingCreatorId
    ? "inline-flex"
    : "none";
  document.getElementById("profile-cancel").style.display = state.editingProfileId
    ? "inline-flex"
    : "none";
  document.getElementById("creator-modal-title").textContent = state.editingCreatorId
    ? "编辑账号"
    : "新增账号";
  document.getElementById("profile-modal-title").textContent = state.editingProfileId
    ? "编辑配置"
    : "新增配置";
}

function applyCreatorQuickAddDefaults(form) {
  const quickAddProfileId = Number(state.panelConfig?.quick_add_profile_id || state.profiles[0]?.id || 1);
  const quickAddTab = state.panelConfig?.quick_add_tab || "post";
  const quickAddAutoEnabled = Boolean(state.panelConfig?.quick_add_auto_download_enabled);
  const quickAddIntervalParts = splitIntervalMinutes(state.panelConfig?.quick_add_auto_download_interval_minutes || 0);
  form.tab.value = quickAddTab;
  form.profile_id.value = String(quickAddProfileId);
  form.auto_download_enabled.checked = quickAddAutoEnabled;
  form.auto_download_interval_value.value = String(quickAddIntervalParts.value);
  form.auto_download_interval_unit.value = quickAddIntervalParts.unit;
}

function resetCreatorForm() {
  const form = getCreatorForm();
  form.reset();
  form.platform.value = "douyin";
  form.enabled.checked = true;
  applyCreatorQuickAddDefaults(form);
  state.editingCreatorId = null;
  updateFormModeText();
}

function resetProfileForm() {
  const form = getProfileForm();
  form.reset();
  form.folder_name.value = "Download";
  setProfileNameFormat(DEFAULT_PROFILE_NAME_FORMAT);
  form.enabled.checked = true;
  state.editingProfileId = null;
  updateFormModeText();
}

function fillCreatorForm(item) {
  const form = getCreatorForm();
  form.platform.value = item.platform;
  form.name.value = item.name;
  form.mark.value = item.mark || "";
  form.url.value = item.url;
  form.sec_user_id.value = item.sec_user_id || "";
  form.tab.value = item.tab || "post";
  form.profile_id.value = String(item.profile_id || 1);
  form.enabled.checked = Boolean(item.enabled);
  form.auto_download_enabled.checked = Boolean(item.auto_download_enabled);
  const intervalParts = splitIntervalMinutes(item.auto_download_interval_minutes || 0);
  form.auto_download_interval_value.value = String(intervalParts.value);
  form.auto_download_interval_unit.value = intervalParts.unit;
  state.editingCreatorId = item.id;
  updateFormModeText();
}

function openModal(element) {
  if (!element) {
    return;
  }
  element.hidden = false;
  document.body.style.overflow = "hidden";
}

function closeModal(element) {
  if (!element) {
    return;
  }
  element.hidden = true;
  if (
    getCreatorModal().hidden &&
    getCreatorDetailModal().hidden &&
    getProfileModal().hidden &&
    getProfileDetailModal().hidden &&
    getScanDetailModal().hidden
  ) {
    document.body.style.overflow = "";
  }
}

function openCreatorModal(item = null) {
  if (item) {
    fillCreatorForm(item);
  } else {
    resetCreatorForm();
  }
  openModal(getCreatorModal());
}

function closeCreatorModal() {
  closeModal(getCreatorModal());
  resetCreatorForm();
}

async function triggerCreatorAutoRun(creatorId) {
  const result = await triggerCreatorAutoRunRequest(creatorId);
  await loadCreators();
  await loadTasks();
  return result;
}

async function runCreatorAutoRunLocked(creatorId, successMessage = "") {
  if (!Number(creatorId)) {
    return false;
  }
  return runLockedAction(`creator:auto-run:${Number(creatorId)}`, async () => {
    const creator = await triggerCreatorAutoRun(Number(creatorId));
    if (successMessage || creator?.auto_download_last_message) {
      notify(
        successMessage || creator?.auto_download_last_message,
        "success",
      );
    }
    return creator;
  });
}

async function triggerCreatorAutoRunRequest(creatorId) {
  state.retryingCreatorIds.add(Number(creatorId));
  renderCreators();
  if (state.viewingCreatorId === Number(creatorId)) {
    const currentItem = state.creators.find((item) => item.id === Number(creatorId));
    if (currentItem) {
      renderCreatorDetail(currentItem);
    }
  }
  try {
    return await request(`/creators/${creatorId}/auto-run`, {
      method: "POST",
      timeoutMs: 30000,
    });
  } finally {
    state.retryingCreatorIds.delete(Number(creatorId));
    renderCreators();
    if (state.viewingCreatorId === Number(creatorId)) {
      const currentItem = state.creators.find((item) => item.id === Number(creatorId));
      if (currentItem) {
        renderCreatorDetail(currentItem);
      }
    }
  }
}

async function resetCreatorAutoSchedule(creatorId) {
  await request(`/creators/${creatorId}/reset-schedule`, {
    method: "POST",
  });
  await loadCreators();
}

async function clearCreatorTaskRecords(creatorId, purgeDownloadHistory = false) {
  const query = purgeDownloadHistory ? "?purge_download_history=true" : "";
  const result = await request(`/creators/${creatorId}/clear-task-records${query}`, {
    method: "POST",
    timeoutMs: 120000,
  });
  await loadCreators();
  await loadTasks();
  return result;
}

function renderAutoHistoryItems(history) {
  const items = Array.isArray(history) ? history : [];
  if (!items.length) {
    return `<div class="auto-history-empty">暂无自动执行历史。</div>`;
  }
  return items.map((entry) => `
    <article class="auto-history-item ${entry.status === "failed" ? "auto-history-item-failed" : ""}">
      <header>
        <strong>${escapeHtml(entry.status === "failed" ? "执行失败" : entry.status === "success" ? "执行成功" : entry.status || "状态未知")}</strong>
        <span>${escapeHtml(entry.run_at || "(未知时间)")}</span>
      </header>
      <p>${escapeHtml(entry.message || "(无执行信息)")}</p>
      <small>下次执行：${escapeHtml(entry.next_run_at || "(未计划)")}</small>
    </article>
  `).join("");
}

function renderCreatorDetail(item) {
  const content = document.getElementById("creator-detail-content");
  const retrying = state.retryingCreatorIds.has(item.id);
  const autoStatus = item.auto_download_enabled ? "已开启" : "已关闭";
  const liveAutoState = getCreatorLiveAutoState(item);
  const autoResultMeta = getAutoResultMeta(liveAutoState.status);
  const autoStatusClass = item.auto_download_enabled
    ? autoResultMeta.cardClass
    : "auto-status-card-muted";
  const autoResultLabel = autoResultMeta.label;
  content.innerHTML = `
    <div class="detail-item detail-item-wide auto-status-card ${autoStatusClass}">
      <div class="auto-status-header">
        <div>
          <strong>自动下载状态</strong>
          <span class="auto-status-title">${autoStatus}</span>
        </div>
        <div class="auto-status-pills">
          <span class="pill ${item.auto_download_enabled ? "pill-accent" : "pill-muted"}">${item.auto_download_enabled ? escapeHtml(formatIntervalLabel(item.auto_download_interval_minutes)) : "未开启"}</span>
          <span class="pill ${autoResultMeta.pillClass}">${escapeHtml(autoResultMeta.label)}</span>
        </div>
      </div>
      <p class="auto-status-message">${escapeHtml(liveAutoState.message || "当前还没有自动执行记录。")}</p>
      <div class="auto-status-meta">
        <span>最近执行：${escapeHtml(item.auto_download_last_run_at || "(暂无)")}</span>
        <span>下次执行：${escapeHtml(item.auto_download_next_run_at || "(未计划)")}</span>
      </div>
      <div class="detail-actions-row">
        <button type="button" class="ghost-button" data-reset-auto-schedule="${item.id}" ${item.auto_download_enabled ? "" : "disabled"}>重置下次执行时间</button>
        <button type="button" class="ghost-button" data-detail-auto-run="${item.id}" ${retrying ? "disabled" : ""}>${retrying ? "提交中..." : "立即入队执行"}</button>
        <button type="button" class="ghost-button danger-ghost-button" data-stop-creator-workflow="${item.id}">结束任务</button>
        <button type="button" class="ghost-button danger-ghost-button" data-clear-task-records="${item.id}">清理下载任务记录</button>
        ${item.auto_download_last_status === "failed"
          ? `<button type="button" class="ghost-button danger-ghost-button" data-detail-auto-retry="${item.id}" ${retrying ? "disabled" : ""}>${retrying ? "重试中..." : "失败重试"}</button>`
          : ""}
      </div>
    </div>
    <div class="detail-item"><strong>显示名称</strong><span>${escapeHtml(item.name)}</span></div>
    <div class="detail-item"><strong>别名 / 备注</strong><span>${escapeHtml(item.mark || "(未填写)")}</span></div>
    <div class="detail-item"><strong>平台</strong><span>${escapeHtml(item.platform)}</span></div>
    <div class="detail-item"><strong>状态</strong><span>${item.enabled ? "启用" : "停用"}</span></div>
    <div class="detail-item"><strong>下载配置</strong><span>${escapeHtml(profileNameById(item.profile_id ?? 1))}</span></div>
    <div class="detail-item"><strong>主页链接</strong><a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">${escapeHtml(item.url)}</a></div>
    <div class="detail-item"><strong>sec_user_id</strong><span>${escapeHtml(item.sec_user_id || "(未填写)")}</span></div>
    <div class="detail-item"><strong>抓取分栏</strong><span>${escapeHtml(formatCreatorTabLabel(item.tab))}</span></div>
    <div class="detail-item"><strong>自动下载</strong><span>${item.auto_download_enabled ? "已开启" : "已关闭"}</span></div>
    <div class="detail-item"><strong>执行周期</strong><span>${escapeHtml(formatIntervalLabel(item.auto_download_interval_minutes))}</span></div>
    <div class="detail-item"><strong>最近执行时间</strong><span>${escapeHtml(item.auto_download_last_run_at || "(暂无)")}</span></div>
    <div class="detail-item"><strong>下次执行时间</strong><span>${escapeHtml(item.auto_download_next_run_at || "(未计划)")}</span></div>
    <div class="detail-item detail-item-wide"><strong>${autoResultLabel}</strong><span>${escapeHtml(liveAutoState.message || "(暂无记录)")}</span></div>
    <div class="detail-item detail-item-wide">
      <strong>最近自动执行记录</strong>
      <div class="auto-history-list">${renderAutoHistoryItems(item.auto_download_history)}</div>
    </div>
    <div class="detail-item"><strong>创建时间</strong><span>${escapeHtml(item.created_at || "(未知)")}</span></div>
    <div class="detail-item"><strong>更新时间</strong><span>${escapeHtml(item.updated_at || "(未知)")}</span></div>
  `;

  const resetButton = content.querySelector("[data-reset-auto-schedule]");
  if (resetButton) {
    resetButton.addEventListener("click", () => runLockedAction(`creator:reset-auto-schedule:${item.id}`, async () => {
      await resetCreatorAutoSchedule(item.id);
      notify("已重置下次执行时间。", "success");
    }));
  }

  const runButton = content.querySelector("[data-detail-auto-run]");
  if (runButton) {
    runButton.addEventListener("click", () => runCreatorAutoRunLocked(
      item.id,
      "已提交后台执行，请稍候查看账号进度。",
    ));
  }

  const retryButton = content.querySelector("[data-detail-auto-retry]");
  if (retryButton) {
    retryButton.addEventListener("click", () => runCreatorAutoRunLocked(
      item.id,
      "已重新提交后台执行，请稍候查看账号进度。",
    ));
  }

  const clearTaskButton = content.querySelector("[data-clear-task-records]");
  if (clearTaskButton) {
    clearTaskButton.addEventListener("click", () => runLockedAction(`creator:clear-task-records:${item.id}`, async () => {
      const confirmed = window.confirm("清理这个账号的下载任务记录，并重置自动下载最近状态吗？\n\n这不会删除已下载记录，也不会删除账号本身。");
      if (!confirmed) {
        return;
      }
      const purgeDownloadHistory = window.confirm("是否一起删除这个账号当前可识别作品的已下载记录？\n\n确定：同时清理已下载记录和扫描缓存，下次会按首次全量状态重新扫描\n取消：只清理任务记录和自动状态");
      const result = await clearCreatorTaskRecords(item.id, purgeDownloadHistory);
      const message = purgeDownloadHistory
        ? `已清理 ${result.deleted_task_count ?? 0} 条任务记录，停止 ${result.stopped_task_count ?? 0} 个未完成任务，删除 ${result.deleted_download_records ?? 0} 条已下载记录（识别作品 ${result.resolved_work_ids ?? 0} 个），并清空 ${result.deleted_scan_cache_count ?? 0} 条扫描缓存。`
        : `已清理 ${result.deleted_task_count ?? 0} 条任务记录，并停止 ${result.stopped_task_count ?? 0} 个未完成任务。`;
      notify(message, "success");
      if (state.viewingCreatorId === item.id) {
        const detail = await request(`/creators/${item.id}`);
        renderCreatorDetail(detail);
      }
    }));
  }

  const stopWorkflowButton = content.querySelector("[data-stop-creator-workflow]");
  if (stopWorkflowButton) {
    stopWorkflowButton.addEventListener("click", () => runLockedAction(`creator:stop-workflow:${item.id}`, async () => {
      const confirmed = window.confirm("结束这个账号当前的扫描、排队和下载任务？\n\n如果正处于扫描请求中，会在本次请求返回后停止，并阻止继续入队。");
      if (!confirmed) {
        return;
      }
      const result = await request(`/creators/${item.id}/stop-workflow`, { method: "POST" });
      notify(result.message || "已发送结束任务请求。", "success");
      await loadCreators();
      await withLoaderLock("load:tasks", loadTasks);
      await loadPollState();
      if (state.viewingCreatorId === item.id) {
        const detail = await request(`/creators/${item.id}`);
        renderCreatorDetail(detail);
      }
    }));
  }
}

async function openCreatorDetailModal(item) {
  state.viewingCreatorId = item.id;
  renderCreatorDetail(item);
  openModal(getCreatorDetailModal());
  try {
    const detail = await request(`/creators/${item.id}`);
    const index = state.creators.findIndex((creator) => creator.id === item.id);
    if (index >= 0) {
      state.creators[index] = {
        ...state.creators[index],
        ...detail,
      };
    }
    if (state.viewingCreatorId === item.id) {
      renderCreatorDetail(detail);
    }
  } catch (error) {
    notify(`账号详情加载失败：${formatError(error)}`, "warning");
  }
}

function closeCreatorDetailModal() {
  state.viewingCreatorId = null;
  closeModal(getCreatorDetailModal());
}

function openProfileModal(item = null) {
  if (item) {
    fillProfileForm(item);
  } else {
    resetProfileForm();
  }
  openModal(getProfileModal());
}

function closeProfileModal() {
  closeModal(getProfileModal());
  resetProfileForm();
}

function renderProfileDetail(item) {
  document.getElementById("profile-detail-content").innerHTML = `
    <div class="detail-item"><strong>配置名称</strong><span>${escapeHtml(item.name)}</span></div>
    <div class="detail-item"><strong>状态</strong><span>${item.enabled ? "启用" : "停用"}</span></div>
    <div class="detail-item detail-item-wide"><strong>下载根目录</strong><span>${escapeHtml(item.root_path || "(跟随引擎默认目录)")}</span></div>
    <div class="detail-item"><strong>文件夹名</strong><span>${escapeHtml(item.folder_name || "Download")}</span></div>
    <div class="detail-item detail-item-wide"><strong>命名格式</strong><span>${escapeHtml(parseProfileNameFormat(item.name_format).map((token) => getProfileNameFormatModule(token).label).join(" / "))}</span></div>
    <div class="detail-item"><strong>按作品建文件夹</strong><span>${Boolean(item.folder_mode) ? "开启" : "关闭"}</span></div>
    <div class="detail-item"><strong>下载音乐</strong><span>${Boolean(item.music) ? "开启" : "关闭"}</span></div>
    <div class="detail-item"><strong>动态封面</strong><span>${Boolean(item.dynamic_cover) ? "开启" : "关闭"}</span></div>
    <div class="detail-item"><strong>静态封面</strong><span>${Boolean(item.static_cover) ? "开启" : "关闭"}</span></div>
  `;
}

function openProfileDetailModal(item) {
  state.viewingProfileId = item.id;
  renderProfileDetail(item);
  openModal(getProfileDetailModal());
}

function closeProfileDetailModal() {
  state.viewingProfileId = null;
  closeModal(getProfileDetailModal());
}

function renderScanDetail(item) {
  document.getElementById("scan-detail-content").innerHTML = `
    ${item.cover ? `<div class="detail-item detail-item-wide"><strong>封面</strong><img class="scan-cover-detail" src="${escapeHtml(item.cover)}" alt="${escapeHtml(item.title)}" /></div>` : ""}
    <div class="detail-item detail-item-wide"><strong>标题</strong><span>${escapeHtml(item.title || "(无标题)")}</span></div>
    <div class="detail-item"><strong>作品 ID</strong><span>${escapeHtml(item.id)}</span></div>
    <div class="detail-item"><strong>类型</strong><span>${escapeHtml(item.type || "(未知)")}</span></div>
    <div class="detail-item"><strong>发布时间</strong><span>${escapeHtml(item.published_at || "未知")}</span></div>
    <div class="detail-item"><strong>下载状态</strong><span>${item.is_downloaded ? "已下载" : "未下载"}</span></div>
    <div class="detail-item detail-item-wide"><strong>原始链接</strong>${item.share_url ? `<a href="${escapeHtml(item.share_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.share_url)}</a>` : `<span>(无)</span>`}</div>
  `;
}

function openScanDetailModal(item) {
  state.viewingScanWorkId = item.id;
  renderScanDetail(item);
  openModal(getScanDetailModal());
}

function closeScanDetailModal() {
  state.viewingScanWorkId = null;
  closeModal(getScanDetailModal());
}

function fillProfileForm(item) {
  const form = getProfileForm();
  form.name.value = item.name;
  form.root_path.value = item.root_path || "";
  form.folder_name.value = item.folder_name || "Download";
  setProfileNameFormat(item.name_format || DEFAULT_PROFILE_NAME_FORMAT);
  form.folder_mode.checked = Boolean(item.folder_mode);
  form.music.checked = Boolean(item.music);
  form.dynamic_cover.checked = Boolean(item.dynamic_cover);
  form.static_cover.checked = Boolean(item.static_cover);
  form.enabled.checked = Boolean(item.enabled);
  state.editingProfileId = item.id;
  updateFormModeText();
}

function fillEngineConfigForm(item) {
  const form = getEngineConfigForm();
  form.desc_length.value = item.desc_length ?? 64;
  form.name_length.value = item.name_length ?? 128;
  form.truncate.value = item.truncate ?? 50;
  form.date_format.value = item.date_format ?? "%Y-%m-%d %H:%M:%S";
  form.split.value = item.split ?? "-";
  form.storage_format.value = item.storage_format ?? "";
  form.run_command.value = item.run_command ?? "";
  form.proxy.value = item.proxy ?? "";
  form.proxy_tiktok.value = item.proxy_tiktok ?? "";
  form.twc_tiktok.value = item.twc_tiktok ?? "";
  form.timeout.value = item.timeout ?? 10;
  form.max_retry.value = item.max_retry ?? 5;
  form.max_pages.value = item.max_pages ?? 0;
  form.max_size.value = item.max_size ?? 0;
  form.chunk.value = item.chunk ?? 2097152;
  form.download.checked = Boolean(item.download);
  form.douyin_platform.checked = Boolean(item.douyin_platform);
  form.tiktok_platform.checked = Boolean(item.tiktok_platform);
  form.cookie.value = item.cookie ?? "";
  form.cookie_tiktok.value = item.cookie_tiktok ?? "";
  form.browser_info.value = JSON.stringify(item.browser_info || {}, null, 2);
  form.browser_info_tiktok.value = JSON.stringify(item.browser_info_tiktok || {}, null, 2);
  form.access_password.value = state.panelConfig?.access_password ?? "151150";
  form.auto_download_scheduler_enabled.checked = Boolean(state.panelConfig?.auto_download_scheduler_enabled);
  form.quick_add_profile_id.value = String(state.panelConfig?.quick_add_profile_id ?? state.profiles[0]?.id ?? 1);
  form.quick_add_tab.value = state.panelConfig?.quick_add_tab ?? "post";
  form.quick_add_auto_download_enabled.checked = Boolean(state.panelConfig?.quick_add_auto_download_enabled);
  const quickAddIntervalParts = splitIntervalMinutes(state.panelConfig?.quick_add_auto_download_interval_minutes || 0);
  form.quick_add_auto_download_interval_value.value = String(quickAddIntervalParts.value);
  form.quick_add_auto_download_interval_unit.value = quickAddIntervalParts.unit;
  form.detail_fetch_concurrency.value = state.panelConfig?.detail_fetch_concurrency ?? 2;
  form.file_download_max_workers.value = state.panelConfig?.file_download_max_workers ?? 4;
  form.initial_account_scan_max_pages.value = state.panelConfig?.initial_account_scan_max_pages ?? 0;
  form.initial_account_scan_timeout_seconds.value = state.panelConfig?.initial_account_scan_timeout_seconds ?? 180;
  form.incremental_account_scan_max_pages.value = state.panelConfig?.incremental_account_scan_max_pages ?? 0;
  form.incremental_account_scan_timeout_seconds.value = state.panelConfig?.incremental_account_scan_timeout_seconds ?? 180;
  form.auto_download_pause_mode.value = state.panelConfig?.auto_download_pause_mode ?? "works";
  form.auto_download_pause_after_works.value = state.panelConfig?.auto_download_pause_after_works ?? 1000;
  form.auto_download_pause_window_minutes.value = state.panelConfig?.auto_download_pause_window_minutes ?? 30;
  form.auto_download_pause_after_creators.value = state.panelConfig?.auto_download_pause_after_creators ?? 10;
  form.auto_download_pause_minutes.value = state.panelConfig?.auto_download_pause_minutes ?? 5;
  form.auto_download_split_batches_enabled.checked = Boolean(state.panelConfig?.auto_download_split_batches_enabled ?? true);
  form.auto_download_work_batch_size.value = state.panelConfig?.auto_download_work_batch_size ?? 20;
  form.risk_guard_enabled.checked = Boolean(state.panelConfig?.risk_guard_enabled);
  form.risk_guard_cooldown_hours.value = state.panelConfig?.risk_guard_cooldown_hours ?? 24;
  form.risk_guard_http_error_streak.value = state.panelConfig?.risk_guard_http_error_streak ?? 3;
  form.risk_guard_status_codes.value = state.panelConfig?.risk_guard_status_codes ?? "403,429";
  form.risk_guard_empty_download_streak.value = state.panelConfig?.risk_guard_empty_download_streak ?? 3;
  form.risk_guard_low_quality_streak.value = state.panelConfig?.risk_guard_low_quality_streak ?? 3;
  form.risk_guard_low_quality_ratio.value = state.panelConfig?.risk_guard_low_quality_ratio ?? 0.8;
  form.risk_guard_low_quality_max_dimension.value = state.panelConfig?.risk_guard_low_quality_max_dimension ?? 720;
  updateAutoDownloadPauseModeFields();
}

function updateAutoDownloadPauseModeFields() {
  const form = getEngineConfigForm();
  if (!form) {
    return;
  }
  const schedulerEnabled = Boolean(form.auto_download_scheduler_enabled?.checked);
  const pauseMode = form.auto_download_pause_mode?.value || "works";
  const splitBatchesEnabled = Boolean(form.auto_download_split_batches_enabled?.checked);
  if (form.auto_download_pause_mode) {
    form.auto_download_pause_mode.disabled = !schedulerEnabled;
  }
  if (form.auto_download_pause_minutes) {
    form.auto_download_pause_minutes.disabled = !schedulerEnabled;
  }
  if (form.auto_download_work_batch_size) {
    form.auto_download_work_batch_size.disabled = !splitBatchesEnabled;
  }
  if (form.auto_download_pause_after_works) {
    form.auto_download_pause_after_works.disabled = !schedulerEnabled || pauseMode !== "works";
  }
  if (form.auto_download_pause_window_minutes) {
    form.auto_download_pause_window_minutes.disabled = !schedulerEnabled || pauseMode !== "works";
  }
  if (form.auto_download_pause_after_creators) {
    form.auto_download_pause_after_creators.disabled = !schedulerEnabled || pauseMode !== "creators";
  }
}

function renderProfiles() {
  const creatorProfileSelect = document.getElementById("creator-profile-select");
  creatorProfileSelect.innerHTML = state.profiles
    .map((item) => `<option value="${item.id}">${escapeHtml(item.name)}</option>`)
    .join("");
  const engineQuickAddProfileSelect = document.getElementById("engine-quick-add-profile-select");
  if (engineQuickAddProfileSelect) {
    engineQuickAddProfileSelect.innerHTML = state.profiles
      .map((item) => `<option value="${item.id}">${escapeHtml(item.name)}</option>`)
      .join("");
    engineQuickAddProfileSelect.value = String(state.panelConfig?.quick_add_profile_id ?? state.profiles[0]?.id ?? "");
  }

  document.getElementById("creator-filter-profile").innerHTML = `
    <option value="">下载配置</option>
    ${state.profiles.map((item) => `<option value="${item.id}">${escapeHtml(item.name)}</option>`).join("")}
  `;
  document.getElementById("creator-filter-profile").value = state.creatorFilters.profileId;
  document.getElementById("creator-filter-auto").value = state.creatorFilters.autoEnabled;
  document.getElementById("creator-filter-auto-status").value = state.creatorFilters.autoStatus;

  const profiles = filteredProfiles();
  const pageData = paginateItems("profiles", profiles);

  document.getElementById("profiles-table").innerHTML = pageData.items
    .map((item) => `
      <article class="list-card compact-card" role="button" tabindex="0" data-detail-profile="${item.id}">
        <header>
          <div>
            <strong>${escapeHtml(item.name)}</strong>
            <span class="subtle-text">${escapeHtml(item.root_path || "跟随引擎默认目录")}</span>
          </div>
          <span class="pill">${item.enabled ? "启用" : "停用"}</span>
        </header>
        <div class="compact-meta">
          <span class="pill">${escapeHtml(item.folder_name || "Download")}</span>
          <span class="pill">${Boolean(item.music) ? "音乐开" : "音乐关"}</span>
          <span class="pill">${Boolean(item.folder_mode) ? "分文件夹" : "平铺"}</span>
        </div>
        <div class="actions">
          <button type="button" data-stop-detail="true" data-edit-profile="${item.id}">编辑</button>
          ${item.id === 1 ? `<button type="button" data-stop-detail="true" class="ghost-button" disabled>默认项</button>` : `<button type="button" data-stop-detail="true" class="ghost-button danger-ghost-button" data-delete-profile="${item.id}">删除</button>`}
        </div>
      </article>
    `)
    .join("") || `<article class="list-card"><small>当前筛选条件下没有配置。</small></article>`;

  renderPagination("profiles-pagination", "profiles", pageData, renderProfiles);

  document.querySelectorAll("[data-detail-profile]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.profiles.find((profile) => profile.id === Number(button.dataset.detailProfile));
      if (item) {
        openProfileDetailModal(item);
      }
    });

    button.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      const item = state.profiles.find((profile) => profile.id === Number(button.dataset.detailProfile));
      if (item) {
        openProfileDetailModal(item);
      }
    });
  });

  document.querySelectorAll("[data-edit-profile]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const item = state.profiles.find((profile) => profile.id === Number(button.dataset.editProfile));
      if (item) {
        openProfileModal(item);
      }
    });
  });

  document.querySelectorAll("[data-delete-profile]").forEach((button) => {
    button.addEventListener("click", (event) => runLockedAction(`profile:delete:${button.dataset.deleteProfile}`, async () => {
      event.stopPropagation();
      const confirmed = window.confirm("删除这个配置后，使用它的账号会自动回退到默认配置。确认继续吗？");
      if (!confirmed) {
        return;
      }
      await request(`/profiles/${button.dataset.deleteProfile}`, { method: "DELETE" });
      await loadProfiles();
      await loadCreators();
    }));
  });
}

function renderCreators() {
  const pageSizeSelect = document.getElementById("creators-page-size");
  if (pageSizeSelect) {
    pageSizeSelect.value = String(state.pagination.creators.pageSize);
  }
  const pageData = {
    items: state.creatorPage.items || [],
    totalItems: state.creatorPage.total || 0,
    totalPages: Math.max(1, Math.ceil((state.creatorPage.total || 0) / state.pagination.creators.pageSize)),
    page: state.pagination.creators.page,
    pageSize: state.pagination.creators.pageSize,
    start: (state.creatorPage.total || 0) ? ((state.pagination.creators.page - 1) * state.pagination.creators.pageSize + 1) : 0,
    end: Math.min(state.pagination.creators.page * state.pagination.creators.pageSize, state.creatorPage.total || 0),
  };
  const failedPageCount = pageData.items.filter((item) => item.auto_download_last_status === "failed").length;
  const retryPageButton = document.getElementById("retry-failed-creators-page");
  retryPageButton.disabled = failedPageCount === 0 || state.retryingFailedPage;
  retryPageButton.textContent = state.retryingFailedPage
    ? "重试中..."
    : failedPageCount
      ? `重试本页失败账号 (${failedPageCount})`
      : "重试本页失败账号";

  document.getElementById("creators-table").innerHTML = pageData.items
    .map((item) => {
      const autoEnabled = Boolean(item.auto_download_enabled);
      const liveAutoState = getCreatorLiveAutoState(item);
      const autoFailed = liveAutoState.status === "failed";
      const retrying = state.retryingCreatorIds.has(item.id);
      const autoResultMeta = getAutoResultMeta(liveAutoState.status);
      const autoStatusLabel = autoEnabled ? `自动 ${escapeHtml(formatIntervalLabel(item.auto_download_interval_minutes))}` : "自动关闭";
      const autoScheduleText = autoEnabled
        ? `下次执行：${escapeHtml(item.auto_download_next_run_at || "(等待计划)")}`
        : "下次执行：已关闭";
      return `
      <article class="list-card creator-card" role="button" tabindex="0" data-detail-creator="${item.id}">
        <header>
          <div>
            <strong>${escapeHtml(creatorDisplayName(item))}</strong>
            <span class="creator-alias">${escapeHtml(item.mark && item.mark !== item.name ? item.name : profileNameById(item.profile_id ?? 1))}</span>
          </div>
          <span class="pill">${escapeHtml(item.platform)}</span>
        </header>
        <div class="creator-meta">
          <span class="pill">${item.enabled ? "启用" : "停用"}</span>
          <span class="pill">${escapeHtml(profileNameById(item.profile_id ?? 1))}</span>
          <span class="pill ${autoEnabled ? "pill-accent" : "pill-muted"}">${autoStatusLabel}</span>
          <span class="pill ${autoResultMeta.pillClass}">${escapeHtml(autoResultMeta.label)}</span>
        </div>
        <small class="creator-schedule-line ${autoEnabled ? "creator-schedule-line-active" : ""}">${autoScheduleText}</small>
        <div class="actions ${autoFailed ? "creator-actions-four" : ""}">
          <button type="button" data-stop-detail="true" data-edit-creator="${item.id}">编辑</button>
          <button type="button" data-stop-detail="true" data-download-creator="${item.id}">全下载</button>
          ${autoFailed ? `<button type="button" data-stop-detail="true" class="ghost-button danger-ghost-button" data-retry-creator-auto="${item.id}" ${retrying ? "disabled" : ""}>${retrying ? "重试中..." : "重试"}</button>` : ""}
          <button type="button" data-stop-detail="true" class="ghost-button danger-ghost-button" data-delete-creator="${item.id}">删除</button>
        </div>
      </article>
    `;
    })
    .join("") || `<article class="list-card"><small>当前筛选条件下没有账号。</small></article>`;

  renderPagination("creators-pagination", "creators", pageData, () => {
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.querySelectorAll("[data-detail-creator]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.creators.find((creator) => creator.id === Number(button.dataset.detailCreator));
      if (item) {
        openCreatorDetailModal(item);
      }
    });

    button.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      const item = state.creators.find((creator) => creator.id === Number(button.dataset.detailCreator));
      if (item) {
        openCreatorDetailModal(item);
      }
    });
  });

  document.querySelectorAll("[data-edit-creator]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const item = state.creators.find((creator) => creator.id === Number(button.dataset.editCreator));
      if (item) {
        openCreatorModal(item);
      }
    });
  });

  document.querySelectorAll("[data-download-creator]").forEach((button) => {
    button.addEventListener("click", (event) => runLockedAction(`creator:download:${button.dataset.downloadCreator}`, async () => {
      event.stopPropagation();
      await request("/tasks", {
        method: "POST",
        body: JSON.stringify({ creator_id: Number(button.dataset.downloadCreator) }),
      });
      await withLoaderLock("load:tasks", loadTasks);
      notify("整号下载任务已启动。", "success");
    }));
  });

  document.querySelectorAll("[data-retry-creator-auto]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      runCreatorAutoRunLocked(
        Number(button.dataset.retryCreatorAuto),
        "已重新提交后台执行，请稍候查看账号进度。",
      );
    });
  });

  document.querySelectorAll("[data-delete-creator]").forEach((button) => {
    button.addEventListener("click", (event) => runLockedAction(`creator:delete:${button.dataset.deleteCreator}`, async () => {
      event.stopPropagation();
      const creatorId = Number(button.dataset.deleteCreator);
      const creator = state.creators.find((item) => item.id === creatorId);
      const confirmed = window.confirm("删除这个账号以及它的扫描缓存吗？");
      if (!confirmed) {
        return;
      }
      const purgeHistory = window.confirm(
        "是否同时清理这个账号可识别的下载记录？\n\n确定：删除账号，并尝试从引擎数据库移除该账号当前可识别作品的下载记录\n取消：只删除账号，不清理下载记录"
      );
      if (purgeHistory) {
        const result = await request(`/creators/${creatorId}/delete-with-history`, {
          method: "POST",
        });
        notify(
          `${creatorDisplayName(creator || { name: `账号 ${creatorId}` })} 已删除，并清理 ${result.deleted_download_records ?? 0} 条下载记录（识别作品 ${result.resolved_work_ids ?? 0} 个）。`,
          "success",
        );
      } else {
        await request(`/creators/${creatorId}`, { method: "DELETE" });
        notify(`${creatorDisplayName(creator || { name: `账号 ${creatorId}` })} 已删除。`, "success");
      }
      await loadCreators();
      await loadCreatorOptions();
      await withLoaderLock("load:tasks", loadTasks);
      await loadPollState();
    }));
  });
}

function renderScan(data) {
  state.currentScan = data;
  state.pagination.scan.page = data.page || state.pagination.scan.page;
  state.pagination.scan.pageSize = data.page_size || state.pagination.scan.pageSize;
  const pageSizeSelect = document.getElementById("scan-page-size");
  if (pageSizeSelect) {
    pageSizeSelect.value = String(state.pagination.scan.pageSize);
  }
  if (state.viewingScanWorkId) {
    const currentItem = (state.currentScan?.items || []).find((item) => String(item.id) === String(state.viewingScanWorkId));
    if (currentItem) {
      renderScanDetail(currentItem);
    } else {
      closeScanDetailModal();
    }
  }
  const typeSelect = document.getElementById("scan-filter-type");
  typeSelect.innerHTML = `
    <option value="">全部类型</option>
    ${(data.available_types || []).map((type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`).join("")}
  `;
  typeSelect.value = state.scanFilters.type;

  const pageData = {
    items: data.items || [],
    totalItems: data.total_visible || 0,
    totalPages: Math.max(1, Math.ceil((data.total_visible || 0) / state.pagination.scan.pageSize)),
    page: state.pagination.scan.page,
    pageSize: state.pagination.scan.pageSize,
    start: (data.total_visible || 0) ? ((state.pagination.scan.page - 1) * state.pagination.scan.pageSize + 1) : 0,
    end: Math.min(state.pagination.scan.page * state.pagination.scan.pageSize, data.total_visible || 0),
  };
  const items = data.items || [];

  document.getElementById("scan-summary").innerHTML = `
    <div class="summary-chip"><strong>${escapeHtml(data.creator_name)}</strong><span>扫描账号</span></div>
    <div class="summary-chip"><strong>${escapeHtml(data.source)}</strong><span>扫描来源</span></div>
    <div class="summary-chip"><strong>${escapeHtml(data.all_count)}</strong><span>作品总数</span></div>
    <div class="summary-chip"><strong>${escapeHtml(data.downloaded_count)}</strong><span>已下载</span></div>
    <div class="summary-chip"><strong>${escapeHtml(data.undownloaded_count)}</strong><span>待下载</span></div>
    <div class="summary-chip"><strong>${escapeHtml(state.selectedWorkIds.size)}</strong><span>已选择</span></div>
  `;

  document.getElementById("scan-results").innerHTML = items
    .map((item) => `
      <article class="work-card scan-card ${state.selectedWorkIds.has(item.id) ? "selected" : ""}" role="button" tabindex="0" data-detail-work="${escapeHtml(item.id)}">
        ${item.cover ? `<img src="${escapeHtml(item.cover)}" alt="${escapeHtml(item.title)}" />` : ""}
        <header>
          <div>
            <strong class="truncate-text" data-tooltip="${escapeHtml(item.title)}">${escapeHtml(item.title)}</strong>
            <span class="subtle-text">${escapeHtml(item.published_at || "未知时间")}</span>
          </div>
          <span class="pill">${escapeHtml(item.type)}</span>
        </header>
        <div class="scan-meta">
          <span class="pill">${item.is_downloaded ? "已下载" : "未下载"}</span>
          <span class="pill">${escapeHtml(item.id)}</span>
        </div>
        <div class="actions">
          <label class="checkbox" data-stop-detail="true">
            <input
              type="checkbox"
              data-toggle-work="${escapeHtml(item.id)}"
              ${state.selectedWorkIds.has(item.id) ? "checked" : ""}
              ${item.is_downloaded ? "disabled" : ""}
            />
            ${item.is_downloaded ? "已下载" : "选择作品"}
          </label>
          ${item.is_downloaded ? `<button type="button" class="ghost-button" data-stop-detail="true" disabled>无需下载</button>` : `<button type="button" data-stop-detail="true" data-download-work="${escapeHtml(item.id)}">下载作品</button>`}
        </div>
      </article>
    `)
    .join("") || `<article class="list-card"><small>当前筛选条件下没有作品。</small></article>`;

  renderPagination("scan-pagination", "scan", pageData, () => {
    loadLatestScan().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.querySelectorAll("[data-detail-work]").forEach((element) => {
    element.addEventListener("click", () => {
      const item = (state.currentScan?.items || []).find((work) => String(work.id) === element.dataset.detailWork);
      if (item) {
        openScanDetailModal(item);
      }
    });

    element.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      const item = (state.currentScan?.items || []).find((work) => String(work.id) === element.dataset.detailWork);
      if (item) {
        openScanDetailModal(item);
      }
    });
  });

  document.querySelectorAll("[data-toggle-work]").forEach((element) => {
    element.addEventListener("change", (event) => {
      event.stopPropagation();
      const workId = event.currentTarget.dataset.toggleWork;
      if (event.currentTarget.checked) {
        state.selectedWorkIds.add(workId);
      } else {
        state.selectedWorkIds.delete(workId);
      }
      renderScan(state.currentScan);
    });
  });

  document.querySelectorAll("[data-download-work]").forEach((button) => {
    button.addEventListener("click", (event) => runLockedAction(`scan:download-work:${button.dataset.downloadWork}`, async () => {
      event.stopPropagation();
      if (!state.currentScan) {
        return;
      }
      await request("/tasks/works", {
        method: "POST",
        body: JSON.stringify({
          creator_id: state.currentScan.creator_id,
          work_ids: [button.dataset.downloadWork],
        }),
      });
      await loadTasks();
      notify("单作品下载任务已启动。", "success");
    }));
  });
}

function renderTasks() {
  const pageSizeSelect = document.getElementById("tasks-page-size");
  if (pageSizeSelect) {
    pageSizeSelect.value = String(state.pagination.tasks.pageSize);
  }
  const statusSelect = document.getElementById("task-filter-status");
  const modeSelect = document.getElementById("task-filter-mode");
  const kindSelect = document.getElementById("task-filter-kind");
  if (statusSelect) {
    statusSelect.value = state.taskFilters.status;
  }
  if (modeSelect) {
    modeSelect.value = state.taskFilters.mode;
  }
  if (kindSelect) {
    kindSelect.value = state.taskFilters.kind;
  }
  const pageData = {
    items: state.taskPage.items || [],
    totalItems: state.taskPage.total || 0,
    totalPages: Math.max(1, Math.ceil((state.taskPage.total || 0) / state.pagination.tasks.pageSize)),
    page: state.pagination.tasks.page,
    pageSize: state.pagination.tasks.pageSize,
    start: (state.taskPage.total || 0) ? ((state.pagination.tasks.page - 1) * state.pagination.tasks.pageSize + 1) : 0,
    end: Math.min(state.pagination.tasks.page * state.pagination.tasks.pageSize, state.taskPage.total || 0),
  };

  document.getElementById("tasks-table").innerHTML = pageData.items
    .map((item) => `
      <article class="list-card compact-card task-card">
        <header>
          <div>
            <strong>${escapeHtml(item.mark || item.creator_name)}</strong>
            <span class="subtle-text">${escapeHtml(item.mark && item.mark !== item.creator_name ? item.creator_name : (item.platform || "未知平台"))}</span>
            <span class="subtle-text">${escapeHtml(item.last_download_at ? `最后下载：${item.last_download_at}` : "最后下载：(暂无)")}</span>
          </div>
          <span class="pill">${escapeHtml(item.platform || "未知平台")}</span>
        </header>
        <div class="compact-meta">
          <span class="pill">视频 ${escapeHtml(item.video_count ?? 0)}</span>
          <span class="pill">图集 ${escapeHtml(item.collection_count ?? 0)}</span>
          <span class="pill">实况 ${escapeHtml(item.live_count ?? 0)}</span>
          <span class="pill ${item.failed_count ? "pill-warn" : "pill-muted"}">失败 ${escapeHtml(item.failed_count ?? 0)}</span>
        </div>
      </article>
    `)
    .join("") || `<article class="list-card"><small>当前筛选条件下没有账号下载统计。</small></article>`;

  renderPagination("tasks-pagination", "tasks", pageData, () => {
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });
}

async function loadHealth() {
  const data = await request("/health");
  const snapshot = buildHealthSnapshot(data);
  if (snapshot === state.snapshots.health) {
    return;
  }
  state.snapshots.health = snapshot;
  state.health = data;
  renderHealth();
}

async function loadEngineConfig() {
  state.engineConfig = await request("/engine/config");
  fillEngineConfigForm(state.engineConfig);
}

async function loadPanelConfig() {
  state.panelConfig = await request("/panel/config");
  if (state.engineConfig) {
    fillEngineConfigForm(state.engineConfig);
  }
}

async function loadAutoDownloadThrottle() {
  const data = await request("/panel/auto-download-throttle");
  const snapshot = buildThrottleSnapshot(data);
  if (snapshot === state.snapshots.throttle) {
    return;
  }
  state.snapshots.throttle = snapshot;
  state.autoDownloadThrottle = data;
  renderHealth();
}

async function loadRiskGuardStatus() {
  const data = await request("/panel/risk-guard");
  const snapshot = buildRiskSnapshot(data);
  if (snapshot === state.snapshots.risk) {
    return;
  }
  state.snapshots.risk = snapshot;
  state.riskGuardStatus = data;
  renderHealth();
}

async function loadProfiles() {
  const data = await request("/profiles");
  const snapshot = buildProfilesSnapshot(data);
  if (snapshot === state.snapshots.profiles) {
    return;
  }
  state.snapshots.profiles = snapshot;
  state.profiles = data;
  if (state.editingProfileId && !state.profiles.some((item) => item.id === state.editingProfileId)) {
    state.editingProfileId = null;
  }
  if (state.viewingProfileId) {
    const currentItem = state.profiles.find((item) => item.id === state.viewingProfileId);
    if (currentItem) {
      renderProfileDetail(currentItem);
    } else {
      closeProfileDetailModal();
    }
  }
  ensureValidPage("profiles", filteredProfiles().length);
  renderProfiles();
  updateFormModeText();
  if (!state.editingCreatorId) {
    resetCreatorForm();
  }
}

async function loadCreatorOptions() {
  const data = await request("/creators/options");
  state.creatorOptions = Array.isArray(data) ? data : [];
  const select = document.getElementById("scan-creator-select");
  if (!select) {
    return;
  }
  const currentValue = select.value;
  select.innerHTML = state.creatorOptions
    .map((item) => `<option value="${item.id}">${escapeHtml(item.mark || item.name)}${item.enabled ? "" : " (停用)"}</option>`)
    .join("");
  if (currentValue && state.creatorOptions.some((item) => String(item.id) === String(currentValue))) {
    select.value = currentValue;
  }
}

function getCurrentScanCreatorId() {
  return document.getElementById("scan-creator-select")?.value || state.currentScan?.creator_id || "";
}

async function loadLatestScan() {
  const creatorId = getCurrentScanCreatorId();
  if (!creatorId) {
    return;
  }
  const query = new URLSearchParams({
    page: String(state.pagination.scan.page),
    page_size: String(state.pagination.scan.pageSize),
  });
  if (state.scanFilters.keyword) {
    query.set("keyword", state.scanFilters.keyword);
  }
  if (state.scanFilters.type) {
    query.set("type", state.scanFilters.type);
  }
  if (state.showDownloaded) {
    query.set("show_downloaded", "true");
  }
  renderScan(await request(`/scans/creator/${creatorId}/latest?${query.toString()}`, {
    timeoutMs: SCAN_REQUEST_TIMEOUT_MS,
  }));
}

async function loadCreators() {
  const query = new URLSearchParams({
    page: String(state.pagination.creators.page),
    page_size: String(state.pagination.creators.pageSize),
  });
  if (state.creatorFilters.keyword) {
    query.set("keyword", state.creatorFilters.keyword);
  }
  if (state.creatorFilters.platform) {
    query.set("platform", state.creatorFilters.platform);
  }
  if (state.creatorFilters.profileId) {
    query.set("profile_id", state.creatorFilters.profileId);
  }
  if (state.creatorFilters.enabled) {
    query.set("enabled", state.creatorFilters.enabled);
  }
  if (state.creatorFilters.autoEnabled) {
    query.set("auto_enabled", state.creatorFilters.autoEnabled);
  }
  if (state.creatorFilters.autoStatus) {
    query.set("auto_status", state.creatorFilters.autoStatus);
  }
  const data = await request(`/creators?${query.toString()}`);
  const snapshot = buildCreatorsSnapshot(data);
  if (snapshot === state.snapshots.creators) {
    return;
  }
  state.snapshots.creators = snapshot;
  state.creators = data.items || [];
  state.creatorPage = {
    items: data.items || [],
    total: data.total || 0,
    page: data.page || state.pagination.creators.page,
    page_size: data.page_size || state.pagination.creators.pageSize,
  };
  state.pagination.creators.page = state.creatorPage.page;
  state.pagination.creators.pageSize = state.creatorPage.page_size;
  await loadCreatorOptions();
  if (state.editingCreatorId && !state.creators.some((item) => item.id === state.editingCreatorId)) {
    state.editingCreatorId = null;
  }
  if (state.viewingCreatorId) {
    const currentItem = state.creators.find((item) => item.id === state.viewingCreatorId);
    if (currentItem) {
      renderCreatorDetail(currentItem);
    } else {
      closeCreatorDetailModal();
    }
  }
  renderCreators();
  renderHealth();
  updateFormModeText();
}

async function loadTasks() {
  const query = new URLSearchParams({
    page: String(state.pagination.tasks.page),
    page_size: String(state.pagination.tasks.pageSize),
  });
  if (state.taskFilters.keyword) {
    query.set("keyword", state.taskFilters.keyword);
  }
  if (state.taskFilters.status) {
    query.set("status", state.taskFilters.status);
  }
  if (state.taskFilters.mode) {
    query.set("mode", state.taskFilters.mode);
  }
  if (state.taskFilters.kind) {
    query.set("kind", state.taskFilters.kind);
  }
  const data = await request(`/tasks/summary?${query.toString()}`);
  const snapshot = buildTasksSnapshot(data);
  if (snapshot === state.snapshots.tasks) {
    return;
  }
  state.snapshots.tasks = snapshot;
  state.tasks = data.items || [];
  state.taskPage = {
    items: data.items || [],
    total: data.total || 0,
    page: data.page || state.pagination.tasks.page,
    page_size: data.page_size || state.pagination.tasks.pageSize,
  };
  state.pagination.tasks.page = state.taskPage.page;
  state.pagination.tasks.pageSize = state.taskPage.page_size;
  renderTasks();
  renderHealth();
}

async function loadPollState() {
  const data = await request("/panel/poll-state");

  const healthSnapshot = buildHealthSnapshot(data.health);
  if (healthSnapshot !== state.snapshots.health) {
    state.snapshots.health = healthSnapshot;
    state.health = data.health;
    renderHealth();
  }

  const manualQueueSnapshot = buildManualQueueSnapshot(data.manual_queue);
  if (manualQueueSnapshot !== state.snapshots.manualQueue) {
    state.snapshots.manualQueue = manualQueueSnapshot;
    state.manualQueue = data.manual_queue || {
      count: 0,
      creator_ids: [],
      next_creator: null,
      path: "",
    };
    renderHealth();
  }

  const throttleSnapshot = buildThrottleSnapshot(data.throttle);
  if (throttleSnapshot !== state.snapshots.throttle) {
    state.snapshots.throttle = throttleSnapshot;
    state.autoDownloadThrottle = data.throttle;
    renderHealth();
  }

  const riskSnapshot = buildRiskSnapshot(data.risk_guard);
  if (riskSnapshot !== state.snapshots.risk) {
    state.snapshots.risk = riskSnapshot;
    state.riskGuardStatus = data.risk_guard;
    renderHealth();
  }

  const dashboardSnapshot = buildDashboardSnapshot(data.dashboard);
  if (dashboardSnapshot !== state.snapshots.dashboard) {
    state.snapshots.dashboard = dashboardSnapshot;
    state.dashboardSummary = data.dashboard || {
      auto_enabled_count: 0,
      auto_failed_count: 0,
      running_auto_tasks: 0,
      next_creator: null,
    };
    renderHealth();
  }

  const runningTasksSnapshot = buildRunningTasksSnapshot(data.running_tasks);
  if (runningTasksSnapshot !== state.snapshots.runningTasks) {
    state.snapshots.runningTasks = runningTasksSnapshot;
    state.runningTaskCards = Array.isArray(data.running_tasks) ? data.running_tasks : [];
    renderHealth();
  }

  const profileDigest = buildDigestSnapshot(data.digests?.profiles);
  if (profileDigest !== state.digests.profiles) {
    state.digests.profiles = profileDigest;
    await loadProfiles();
  }

  const creatorDigest = buildDigestSnapshot(data.digests?.creators);
  if (creatorDigest !== state.digests.creators) {
    state.digests.creators = creatorDigest;
    await loadCreators();
    await loadCreatorOptions();
  }

  const taskDigest = buildDigestSnapshot(data.digests?.tasks);
  if (taskDigest !== state.digests.tasks) {
    state.digests.tasks = taskDigest;
    if (document.getElementById("tasks")?.classList.contains("active")) {
      await loadTasks();
    }
  }
}

function stopBackgroundPolling() {
  if (backgroundPollHandle) {
    window.clearTimeout(backgroundPollHandle);
    backgroundPollHandle = null;
  }
}

function scheduleBackgroundPolling() {
  stopBackgroundPolling();
  backgroundPollHandle = window.setTimeout(async () => {
    if (document.hidden) {
      scheduleBackgroundPolling();
      return;
    }
    if (pollingSuspendedUntil > Date.now()) {
      state.network.pollingPausedUntil = pollingSuspendedUntil;
      renderNetworkStatus();
      scheduleBackgroundPolling();
      return;
    }
    pollingSuspendedUntil = 0;
    state.network.pollingPausedUntil = 0;
    await Promise.allSettled([
      withLoaderLock("poll:state", loadPollState),
    ]).then((results) => {
      const failedCount = results.filter((item) => item.status === "rejected").length;
      if (failedCount) {
        consecutivePollFailures += 1;
        if (consecutivePollFailures >= MAX_CONSECUTIVE_POLL_FAILURES) {
          suspendBackgroundPolling(`${consecutivePollFailures} 轮轮询连续失败`);
          consecutivePollFailures = 0;
        }
      } else {
        consecutivePollFailures = 0;
      }
    });
    scheduleBackgroundPolling();
  }, POLL_INTERVAL_MS);
}

function bindNavigation() {
  document.querySelectorAll(".nav-link").forEach((button) => {
    button.addEventListener("click", () => {
      switchView(button.dataset.view);
    });
  });
}

function bindForms() {
  document.getElementById("creator-cancel").addEventListener("click", () => {
    closeCreatorModal();
  });

  document.getElementById("profile-cancel").addEventListener("click", () => {
    closeProfileModal();
  });

  getCreatorForm().addEventListener("submit", (event) => runLockedAction("form:creator", async () => {
    event.preventDefault();
    const form = event.currentTarget;
    const isEditing = Boolean(state.editingCreatorId);
    const currentCreator = isEditing
      ? state.creators.find((item) => item.id === state.editingCreatorId)
      : null;
    await request(isEditing ? `/creators/${state.editingCreatorId}` : "/creators", {
      method: isEditing ? "PUT" : "POST",
      body: JSON.stringify({
        platform: form.platform.value,
        name: form.name.value,
        mark: form.mark.value,
        url: form.url.value,
        sec_user_id: form.sec_user_id.value,
        tab: form.tab.value,
        profile_id: Number(form.profile_id.value || 1),
        enabled: boolValue(form, "enabled"),
        auto_download_enabled: boolValue(form, "auto_download_enabled"),
        auto_download_interval_minutes: boolValue(form, "auto_download_enabled")
          ? joinIntervalMinutes(form.auto_download_interval_value.value, form.auto_download_interval_unit.value)
          : 0,
        auto_download_start_at: null,
        auto_download_last_run_at: currentCreator?.auto_download_last_run_at || null,
        auto_download_next_run_at: currentCreator?.auto_download_next_run_at || null,
        auto_download_last_status: currentCreator?.auto_download_last_status || "",
        auto_download_last_message: currentCreator?.auto_download_last_message || "",
      }),
    });
    closeCreatorModal();
    await loadCreators();
    notify(isEditing ? "账号已更新。" : "账号已新增。", "success");
  }));

  getProfileForm().addEventListener("submit", (event) => runLockedAction("form:profile", async () => {
    event.preventDefault();
    const form = event.currentTarget;
    const isEditing = Boolean(state.editingProfileId);
    syncProfileNameFormatInput();
    await request(isEditing ? `/profiles/${state.editingProfileId}` : "/profiles", {
      method: isEditing ? "PUT" : "POST",
      body: JSON.stringify({
        name: form.name.value,
        root_path: form.root_path.value,
        folder_name: form.folder_name.value,
        name_format: serializeProfileNameFormat(),
        folder_mode: boolValue(form, "folder_mode"),
        music: boolValue(form, "music"),
        dynamic_cover: boolValue(form, "dynamic_cover"),
        static_cover: boolValue(form, "static_cover"),
        enabled: boolValue(form, "enabled"),
      }),
    });
    closeProfileModal();
    await loadProfiles();
    await loadCreators();
    notify(isEditing ? "配置已更新。" : "配置已新增。", "success");
  }));

  getEngineConfigForm().addEventListener("submit", (event) => {
    event.preventDefault();
  });
}

function bindActions() {
  document.getElementById("open-creator-create").addEventListener("click", () => {
    openCreatorModal();
  });

  document.getElementById("creator-quick-add-submit").addEventListener("click", () => runLockedAction("creator:quick-add", async () => {
    const input = document.getElementById("creator-quick-add-url");
    const text = input?.value?.trim() || "";
    if (!text) {
      notify("请先输入账号主页链接，每行一个。", "warning");
      return;
    }
    const result = await request("/creators/quick-add", {
      method: "POST",
      body: JSON.stringify({ url: text }),
      timeoutMs: 120000,
    });
    const failedItems = Array.isArray(result.items) ? result.items.filter((item) => !item.success) : [];
    if (input) {
      input.value = failedItems.map((item) => item.url || "").filter(Boolean).join("\n");
    }
    await loadCreators();
    await loadCreatorOptions();
    const summary = [
      `本次共处理 ${result.total || 0} 条链接。`,
      `成功新增 ${result.created_count || 0} 个账号。`,
      failedItems.length ? `失败 ${failedItems.length} 条。` : "",
      failedItems.length ? "失败链接已保留在输入框中，便于直接修正后重试。" : "所有链接已处理完成，输入框已自动清空。",
      failedItems.length ? `失败明细：${failedItems.map((item) => `${item.url} -> ${item.error || "未知错误"}`).join("；")}` : "",
    ].filter(Boolean).join("\n");
    notify(
      summary,
      failedItems.length ? "warning" : "success",
    );
  }));

  document.getElementById("retry-failed-creators-page").addEventListener("click", () => runLockedAction("retry:failed-creators-page", async () => {
    const creatorIds = currentCreatorPageItems()
      .filter((item) => item.auto_download_last_status === "failed")
      .map((item) => item.id);
    if (!creatorIds.length) {
      notify("当前页没有可重试的失败账号。", "warning");
      return;
    }
    state.retryingFailedPage = true;
    renderCreators();
    try {
      const results = [];
      const requestFailures = [];
      for (const creatorId of creatorIds) {
        try {
          results.push(await triggerCreatorAutoRunRequest(creatorId));
        } catch (error) {
          const creator = state.creators.find((item) => item.id === creatorId);
          requestFailures.push(`${creator ? creatorDisplayName(creator) : `账号 ${creatorId}`}：${formatError(error)}`);
        }
      }
      await loadCreators();
      await withLoaderLock("load:tasks", loadTasks);
      const queuedCount = results.filter((item) => ["queued", "running", "scheduled"].includes(item?.auto_download_last_status)).length;
      const summary = [
        `本页共尝试重试 ${creatorIds.length} 个失败账号。`,
        `已成功提交 ${results.length} 个请求。`,
        queuedCount ? `其中 ${queuedCount} 个账号已进入排队或执行状态。` : "账号状态会在后台扫描后逐步更新。",
        requestFailures.length ? `请求报错 ${requestFailures.length} 个。` : "",
        requestFailures.length ? `请求失败详情：${requestFailures.join("；")}` : "",
      ].filter(Boolean).join("\n");
      notify(
        summary,
        requestFailures.length ? "warning" : "success",
      );
    } finally {
      state.retryingFailedPage = false;
      renderCreators();
    }
  }));

  document.getElementById("creator-test-auto").addEventListener("click", () => runCreatorAutoRunLocked(
    state.editingCreatorId || 0,
    "已提交后台执行，请稍候查看账号进度。",
  ));

  document.getElementById("open-profile-create").addEventListener("click", () => {
    openProfileModal();
  });

  document.getElementById("profile-name-format-plus").addEventListener("click", (event) => {
    event.stopPropagation();
    const picker = document.getElementById("profile-name-format-picker");
    if (picker.hidden) {
      openProfileNameFormatPicker();
      return;
    }
    closeProfileNameFormatPicker();
  });

  document.getElementById("profile-name-format-picker").addEventListener("click", (event) => {
    const button = event.target.closest("[data-name-format-add]");
    if (!button) {
      return;
    }
    addProfileNameFormatToken(button.dataset.nameFormatAdd);
    closeProfileNameFormatPicker();
  });

  document.getElementById("profile-name-format-list").addEventListener("click", (event) => {
    const target = event.target.closest("button");
    if (!target) {
      return;
    }
    if (target.dataset.nameFormatRemove) {
      removeProfileNameFormatToken(Number(target.dataset.nameFormatRemove));
    }
  });

  document.getElementById("profile-name-format-list").addEventListener("dragstart", (event) => {
    const chip = event.target.closest("[data-name-format-index]");
    if (!chip) {
      return;
    }
    draggingProfileNameFormatIndex = Number(chip.dataset.nameFormatIndex);
    chip.classList.add("name-format-chip-dragging");
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", String(draggingProfileNameFormatIndex));
  });

  document.getElementById("profile-name-format-list").addEventListener("dragend", (event) => {
    const chip = event.target.closest("[data-name-format-index]");
    if (chip) {
      chip.classList.remove("name-format-chip-dragging");
    }
    draggingProfileNameFormatIndex = null;
    document.querySelectorAll(".name-format-chip-drop-target").forEach((item) => item.classList.remove("name-format-chip-drop-target"));
  });

  document.getElementById("profile-name-format-list").addEventListener("dragover", (event) => {
    const chip = event.target.closest("[data-name-format-index]");
    if (!chip) {
      return;
    }
    event.preventDefault();
    document.querySelectorAll(".name-format-chip-drop-target").forEach((item) => item.classList.remove("name-format-chip-drop-target"));
    chip.classList.add("name-format-chip-drop-target");
    event.dataTransfer.dropEffect = "move";
  });

  document.getElementById("profile-name-format-list").addEventListener("dragleave", (event) => {
    const chip = event.target.closest("[data-name-format-index]");
    if (chip) {
      chip.classList.remove("name-format-chip-drop-target");
    }
  });

  document.getElementById("profile-name-format-list").addEventListener("drop", (event) => {
    const chip = event.target.closest("[data-name-format-index]");
    if (!chip) {
      return;
    }
    event.preventDefault();
    chip.classList.remove("name-format-chip-drop-target");
    moveProfileNameFormatTokenToIndex(draggingProfileNameFormatIndex, Number(chip.dataset.nameFormatIndex));
  });

  document.getElementById("reload-engine-config").addEventListener("click", () => runLockedAction("engine:reload-config", async () => {
    await loadPanelConfig();
    await loadEngineConfig();
  }));

  getEngineConfigForm().auto_download_pause_mode.addEventListener("change", () => {
    updateAutoDownloadPauseModeFields();
  });
  getEngineConfigForm().auto_download_scheduler_enabled.addEventListener("change", () => {
    updateAutoDownloadPauseModeFields();
  });
  getEngineConfigForm().auto_download_split_batches_enabled.addEventListener("change", () => {
    updateAutoDownloadPauseModeFields();
  });

  document.getElementById("save-engine-config").addEventListener("click", () => runLockedAction("engine:save-config", async () => {
    const form = getEngineConfigForm();
    const payload = {
      desc_length: Number(form.desc_length.value || 64),
      name_length: Number(form.name_length.value || 128),
      truncate: Number(form.truncate.value || 50),
      date_format: form.date_format.value,
      split: form.split.value,
      storage_format: form.storage_format.value,
      run_command: form.run_command.value,
      proxy: form.proxy.value,
      proxy_tiktok: form.proxy_tiktok.value,
      twc_tiktok: form.twc_tiktok.value,
      timeout: Number(form.timeout.value || 10),
      max_retry: Number(form.max_retry.value || 5),
      max_pages: Number(form.max_pages.value || 0),
      max_size: Number(form.max_size.value || 0),
      chunk: Number(form.chunk.value || 2097152),
      download: boolValue(form, "download"),
      douyin_platform: boolValue(form, "douyin_platform"),
      tiktok_platform: boolValue(form, "tiktok_platform"),
      cookie: form.cookie.value,
      cookie_tiktok: form.cookie_tiktok.value,
      browser_info: parseJsonField(form.browser_info.value, "browser_info"),
      browser_info_tiktok: parseJsonField(form.browser_info_tiktok.value, "browser_info_tiktok"),
    };
    const panelPayload = {
      access_password: form.access_password.value.trim() || "151150",
      auto_download_scheduler_enabled: boolValue(form, "auto_download_scheduler_enabled"),
      quick_add_profile_id: Number(form.quick_add_profile_id.value || state.profiles[0]?.id || 1),
      quick_add_tab: form.quick_add_tab.value || "post",
      quick_add_auto_download_enabled: boolValue(form, "quick_add_auto_download_enabled"),
      quick_add_auto_download_interval_minutes: boolValue(form, "quick_add_auto_download_enabled")
        ? joinIntervalMinutes(form.quick_add_auto_download_interval_value.value, form.quick_add_auto_download_interval_unit.value)
        : 0,
      detail_fetch_concurrency: Number(form.detail_fetch_concurrency.value || 2),
      file_download_max_workers: Number(form.file_download_max_workers.value || 4),
      initial_account_scan_max_pages: Number(form.initial_account_scan_max_pages.value || 0),
      initial_account_scan_timeout_seconds: Number(form.initial_account_scan_timeout_seconds.value || 180),
      incremental_account_scan_max_pages: Number(form.incremental_account_scan_max_pages.value || 0),
      incremental_account_scan_timeout_seconds: Number(form.incremental_account_scan_timeout_seconds.value || 180),
      auto_download_pause_mode: form.auto_download_pause_mode.value || "works",
      auto_download_pause_after_works: Number(form.auto_download_pause_after_works.value || 0),
      auto_download_pause_window_minutes: Number(form.auto_download_pause_window_minutes.value || 30),
      auto_download_pause_after_creators: Number(form.auto_download_pause_after_creators.value || 0),
      auto_download_pause_minutes: Number(form.auto_download_pause_minutes.value || 5),
      auto_download_split_batches_enabled: boolValue(form, "auto_download_split_batches_enabled"),
      auto_download_work_batch_size: Number(form.auto_download_work_batch_size.value || 20),
      risk_guard_enabled: boolValue(form, "risk_guard_enabled"),
      risk_guard_cooldown_hours: Number(form.risk_guard_cooldown_hours.value || 24),
      risk_guard_http_error_streak: Number(form.risk_guard_http_error_streak.value || 3),
      risk_guard_status_codes: form.risk_guard_status_codes.value,
      risk_guard_empty_download_streak: Number(form.risk_guard_empty_download_streak.value || 3),
      risk_guard_low_quality_streak: Number(form.risk_guard_low_quality_streak.value || 3),
      risk_guard_low_quality_ratio: Number(form.risk_guard_low_quality_ratio.value || 0.8),
      risk_guard_low_quality_max_dimension: Number(form.risk_guard_low_quality_max_dimension.value || 720),
    };
    state.engineConfig = await request("/engine/config", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    state.panelConfig = await request("/panel/config", {
      method: "PUT",
      body: JSON.stringify(panelPayload),
    });
    window.sessionStorage.setItem(ACCESS_STORAGE_KEY, state.panelConfig.access_password);
    fillEngineConfigForm(state.engineConfig);
    await loadRiskGuardStatus();
    notify("引擎配置、页面访问密码、详情提取并发、文件下载并发、首次全量与后续增量扫描策略、自动下载暂停策略、批次拆分设置和风控兜底已保存。", "success");
  }));

  document.getElementById("run-scan").addEventListener("click", () => runLockedAction("scan:run", async () => {
    const creatorId = document.getElementById("scan-creator-select").value;
    if (!creatorId) {
      notify("请先至少新增一个账号。", "warning");
      return;
    }
    state.selectedWorkIds.clear();
    state.pagination.scan.page = 1;
    const query = new URLSearchParams({
      page: String(state.pagination.scan.page),
      page_size: String(state.pagination.scan.pageSize),
    });
    if (state.scanFilters.keyword) {
      query.set("keyword", state.scanFilters.keyword);
    }
    if (state.scanFilters.type) {
      query.set("type", state.scanFilters.type);
    }
    if (state.showDownloaded) {
      query.set("show_downloaded", "true");
    }
    renderScan(await request(`/scans/creator/${creatorId}?${query.toString()}`, {
      method: "POST",
      timeoutMs: SCAN_REQUEST_TIMEOUT_MS,
    }));
  }));

  document.getElementById("show-downloaded").addEventListener("change", (event) => {
    state.showDownloaded = event.currentTarget.checked;
    state.pagination.scan.page = 1;
    if (state.currentScan) {
      loadLatestScan().catch((error) => {
        console.error(error);
        notify(formatError(error), "error");
      });
    }
  });

  document.getElementById("scan-filter-keyword").addEventListener("input", (event) => {
    state.scanFilters.keyword = event.currentTarget.value;
    state.pagination.scan.page = 1;
    if (state.currentScan) {
      loadLatestScan().catch((error) => {
        console.error(error);
        notify(formatError(error), "error");
      });
    }
  });

  document.getElementById("scan-filter-type").addEventListener("change", (event) => {
    state.scanFilters.type = event.currentTarget.value;
    state.pagination.scan.page = 1;
    if (state.currentScan) {
      loadLatestScan().catch((error) => {
        console.error(error);
        notify(formatError(error), "error");
      });
    }
  });

  document.getElementById("select-all-visible").addEventListener("click", () => {
    currentScanPageItems().forEach((item) => {
      if (!item.is_downloaded) {
        state.selectedWorkIds.add(item.id);
      }
    });
    if (state.currentScan) {
      renderScan(state.currentScan);
    }
  });

  document.getElementById("clear-selection").addEventListener("click", () => {
    state.selectedWorkIds.clear();
    if (state.currentScan) {
      renderScan(state.currentScan);
    }
  });

  document.getElementById("download-selected").addEventListener("click", () => runLockedAction("scan:download-selected", async () => {
    if (!state.currentScan || !state.selectedWorkIds.size) {
      return;
    }
    await request("/tasks/works", {
      method: "POST",
      body: JSON.stringify({
        creator_id: state.currentScan.creator_id,
        work_ids: [...state.selectedWorkIds],
      }),
    });
    state.selectedWorkIds.clear();
    await withLoaderLock("load:tasks", loadTasks);
    notify("选中作品下载任务已启动。", "success");
  }));

  document.getElementById("download-scanned").addEventListener("click", () => runLockedAction("scan:download-scanned", async () => {
    if (!state.currentScan || !state.currentScan.items.length) {
      return;
    }
    await request("/tasks/works", {
      method: "POST",
      body: JSON.stringify({
        creator_id: state.currentScan.creator_id,
        work_ids: state.currentScan.items.map((item) => item.id),
      }),
    });
    await withLoaderLock("load:tasks", loadTasks);
    notify("当前未下载作品已加入任务。", "success");
  }));

  document.getElementById("sync-engine").addEventListener("click", () => runLockedAction("engine:sync", async () => {
    await request("/engine/settings-sync", { method: "POST" });
    await loadHealth();
    notify("已同步到引擎 settings.json。", "success");
  }));

  document.getElementById("refresh-creators").addEventListener("click", () => runLockedAction("refresh:creators", loadCreators));
  document.getElementById("refresh-profiles").addEventListener("click", () => runLockedAction("refresh:profiles", loadProfiles));
  document.getElementById("refresh-tasks").addEventListener("click", () => runLockedAction("refresh:tasks", () => withLoaderLock("load:tasks", loadTasks)));

  document.getElementById("creator-filter-keyword").addEventListener("input", (event) => {
    state.creatorFilters.keyword = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("creator-filter-platform").addEventListener("change", (event) => {
    state.creatorFilters.platform = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("creator-filter-profile").addEventListener("change", (event) => {
    state.creatorFilters.profileId = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("creator-filter-enabled").addEventListener("change", (event) => {
    state.creatorFilters.enabled = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("creator-filter-auto").addEventListener("change", (event) => {
    state.creatorFilters.autoEnabled = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("creator-filter-auto-status").addEventListener("change", (event) => {
    state.creatorFilters.autoStatus = event.currentTarget.value;
    state.pagination.creators.page = 1;
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("profile-filter-keyword").addEventListener("input", (event) => {
    state.profileFilters.keyword = event.currentTarget.value;
    state.pagination.profiles.page = 1;
    renderProfiles();
  });

  document.getElementById("profile-filter-enabled").addEventListener("change", (event) => {
    state.profileFilters.enabled = event.currentTarget.value;
    state.pagination.profiles.page = 1;
    renderProfiles();
  });

  document.getElementById("task-filter-keyword").addEventListener("input", (event) => {
    state.taskFilters.keyword = event.currentTarget.value;
    state.pagination.tasks.page = 1;
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("task-filter-status").addEventListener("change", (event) => {
    state.taskFilters.status = event.currentTarget.value;
    state.pagination.tasks.page = 1;
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("task-filter-mode").addEventListener("change", (event) => {
    state.taskFilters.mode = event.currentTarget.value;
    state.pagination.tasks.page = 1;
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("task-filter-kind").addEventListener("change", (event) => {
    state.taskFilters.kind = event.currentTarget.value;
    state.pagination.tasks.page = 1;
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("profiles-page-size").addEventListener("change", (event) => {
    setPageSize("profiles", Number(event.currentTarget.value));
    renderProfiles();
  });

  document.getElementById("creators-page-size").addEventListener("change", (event) => {
    setPageSize("creators", Number(event.currentTarget.value));
    loadCreators().catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.getElementById("scan-page-size").addEventListener("change", (event) => {
    setPageSize("scan", Number(event.currentTarget.value));
    if (state.currentScan) {
      loadLatestScan().catch((error) => {
        console.error(error);
        notify(formatError(error), "error");
      });
    }
  });

  document.getElementById("tasks-page-size").addEventListener("change", (event) => {
    setPageSize("tasks", Number(event.currentTarget.value));
    withLoaderLock("load:tasks", loadTasks).catch((error) => {
      console.error(error);
      notify(formatError(error), "error");
    });
  });

  document.querySelectorAll("[data-close-modal]").forEach((element) => {
    element.addEventListener("click", () => {
      if (element.dataset.closeModal === "creator") {
        closeCreatorModal();
        return;
      }
      if (element.dataset.closeModal === "creator-detail") {
        closeCreatorDetailModal();
        return;
      }
      if (element.dataset.closeModal === "profile") {
        closeProfileModal();
        return;
      }
      if (element.dataset.closeModal === "profile-detail") {
        closeProfileDetailModal();
        return;
      }
      if (element.dataset.closeModal === "scan-detail") {
        closeScanDetailModal();
        return;
      }
    });
  });

  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") {
      return;
    }
    if (!getCreatorDetailModal().hidden) {
      closeCreatorDetailModal();
    }
    if (!getCreatorModal().hidden) {
      closeCreatorModal();
    }
    if (!getProfileDetailModal().hidden) {
      closeProfileDetailModal();
    }
    if (!getProfileModal().hidden) {
      closeProfileModal();
    }
    if (!getScanDetailModal().hidden) {
      closeScanDetailModal();
    }
    closeProfileNameFormatPicker();
  });

  document.addEventListener("click", (event) => {
    const picker = document.getElementById("profile-name-format-picker");
    const plusButton = document.getElementById("profile-name-format-plus");
    if (!picker || !plusButton || picker.hidden) {
      return;
    }
    if (picker.contains(event.target) || plusButton.contains(event.target)) {
      return;
    }
    closeProfileNameFormatPicker();
  });
}

async function bootstrap() {
  bindNavigation();
  bindForms();
  bindActions();
  updateFormModeText();
  resetProfileForm();
  await loadHealth();
  await loadPanelConfig();
  await loadAutoDownloadThrottle();
  await loadRiskGuardStatus();
  await loadEngineConfig();
  await loadProfiles();
  await loadCreatorOptions();
  await loadCreators();
  await loadPollState();
  scheduleBackgroundPolling();

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopBackgroundPolling();
      return;
    }
    pollingSuspendedUntil = 0;
    state.network.pollingPausedUntil = 0;
    Promise.allSettled([
      withLoaderLock("poll:state", loadPollState),
    ]).finally(() => {
      scheduleBackgroundPolling();
    });
  });
}

function hasAccess() {
  return window.sessionStorage.getItem(ACCESS_STORAGE_KEY) === (state.panelConfig?.access_password ?? "151150");
}

function unlockAccess() {
  window.sessionStorage.setItem(ACCESS_STORAGE_KEY, state.panelConfig?.access_password ?? "151150");
  document.body.classList.remove("auth-locked");
}

async function bindAuthGate() {
  const form = document.getElementById("auth-form");
  const passwordInput = document.getElementById("auth-password");
  const errorText = document.getElementById("auth-error");

  if (!form || !passwordInput || !errorText) {
    return;
  }

  state.panelConfig = await request("/panel/config");

  if (hasAccess()) {
    unlockAccess();
    return;
  }

  passwordInput.focus();

  return new Promise((resolve) => {
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const password = passwordInput.value.trim();
      if (password === (state.panelConfig?.access_password ?? "151150")) {
        errorText.hidden = true;
        unlockAccess();
        resolve();
        return;
      }
      errorText.hidden = false;
      passwordInput.select();
    });

    passwordInput.addEventListener("input", () => {
      if (!errorText.hidden) {
        errorText.hidden = true;
      }
    });
  });
}

async function startApplication() {
  await bindAuthGate();
  await bootstrap();
}

startApplication().catch((error) => {
  console.error(error);
  if (!document.body.classList.contains("auth-locked")) {
    notify(`面板启动失败：${formatError(error)}`, "error");
  }
});


