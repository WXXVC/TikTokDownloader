// ==UserScript==
// @name         TKWEB 一键新增账号
// @namespace    https://github.com/
// @version      0.1.0
// @description  在抖音/TikTok 账号主页一键新增账号到 TKWEB WebUI
// @match        https://www.douyin.com/*
// @match        https://www.tiktok.com/@*
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @grant        GM_xmlhttpRequest
// @connect      *
// ==/UserScript==

(function () {
  "use strict";

  const CONFIG_KEY = "tkweb-script-config";
  const CACHE_KEY = "tkweb-script-cache";
  const HISTORY_KEY = "tkweb-script-history";
  const PANEL_PATH = "/panel-api/creators/script-upsert";
  const HISTORY_PATH = "/panel-api/creators/script-history";
  const BUTTON_ID = "tkweb-add-creator-button";
  const BADGE_ID = "tkweb-add-creator-badge";

  function readConfig() {
    return {
      baseUrl: "http://127.0.0.1:5555",
      password: "151150",
      ...JSON.parse(GM_getValue(CONFIG_KEY, "{}") || "{}"),
    };
  }

  function saveConfig(config) {
    GM_setValue(CONFIG_KEY, JSON.stringify(config));
  }

  function readCache() {
    return JSON.parse(GM_getValue(CACHE_KEY, "{}") || "{}");
  }

  function saveCache(cache) {
    GM_setValue(CACHE_KEY, JSON.stringify(cache));
  }

  function readHistoryStore() {
    return JSON.parse(GM_getValue(HISTORY_KEY, "{}") || "{}");
  }

  function saveHistoryStore(store) {
    GM_setValue(HISTORY_KEY, JSON.stringify(store));
  }

  function normalizeUrl(url) {
    try {
      const parsed = new URL(url, window.location.origin);
      parsed.search = "";
      parsed.hash = "";
      return parsed.toString().replace(/\/$/, "");
    } catch {
      return String(url || "").trim().replace(/\/$/, "");
    }
  }

  function getPlatform() {
    return location.hostname.includes("tiktok.com") ? "tiktok" : "douyin";
  }

  function cleanCreatorName(text) {
    return String(text || "")
      .replace(/^@\s*/, "")
      .replace(/\s*·.*$/, "")
      .replace(/\s+\d[\d.]*[万亿]?\s*粉丝.*$/, "")
      .replace(/\s+\d[\d.]*[万亿]?\s*获赞.*$/, "")
      .replace(/\s*(粉丝|获赞).*/, "")
      .trim();
  }

  function getProfilePageCreatorName() {
    const selectors = [
      'h1[data-e2e="user-title"]',
      '[data-e2e="user-title"]',
      'h1',
      'h1 span',
      'h2[data-e2e="user-subtitle"]',
      '[data-e2e="browse-user-title"]',
    ];
    for (const selector of selectors) {
      const element = document.querySelector(selector);
      const text = element?.textContent?.trim();
      if (text) {
        return cleanCreatorName(text);
      }
    }
    return cleanCreatorName(document.title
      .replace(/的抖音\s*[|-].*$/, "")
      .replace(/\s*[|-].*$/, "")
      .trim()) || "未命名账号";
  }

  function isProfilePage() {
    return /^\/user\//.test(location.pathname);
  }

  function findNearbyAuthorText(anchor) {
    const containers = [anchor, anchor.parentElement, anchor.closest("[role='dialog']"), anchor.closest("section"), anchor.closest("div")]
      .filter(Boolean);
    for (const container of containers) {
      const text = container?.textContent || "";
      const atIndexes = [...text.matchAll(/@/g)].map((match) => match.index ?? -1).filter((index) => index >= 0);
      for (let index = atIndexes.length - 1; index >= 0; index -= 1) {
        const start = atIndexes[index];
        const slice = text.slice(start, start + 48);
        const dotIndex = slice.indexOf("·");
        if (dotIndex <= 0) {
          continue;
        }
        const candidate = cleanCreatorName(slice.slice(1, dotIndex));
        if (candidate && !/粉丝|获赞/.test(candidate)) {
          return candidate;
        }
      }
    }
    return "";
  }

  function getAuthorNameFromAnchor(anchor) {
    const directText = Array.from(anchor.childNodes || [])
      .filter((node) => node.nodeType === Node.TEXT_NODE)
      .map((node) => node.textContent || "")
      .join(" ")
      .trim();
    const cleanedDirectText = cleanCreatorName(directText);
    if (cleanedDirectText && !/粉丝|获赞/.test(cleanedDirectText)) {
      return cleanedDirectText;
    }
    return "";
  }

  function scoreAuthorLink(anchor) {
    const href = anchor.getAttribute("href") || "";
    if (!/\/user\//.test(href) || /\/user\/self/.test(href)) {
      return -1;
    }
    let score = 0;
    const text = String(anchor.textContent || "").trim();
    if (text.includes("@")) {
      score += 5;
    }
    if (/粉丝|获赞/.test(text)) {
      score += 3;
    }
    if (/enter_method=video_title|from_gid=|vid=/.test(href)) {
      score += 3;
    }
    if (anchor.closest("[role='dialog']")) {
      score += 4;
    }
    const nearbyName = findNearbyAuthorText(anchor);
    if (nearbyName) {
      score += 4;
    }
    return score;
  }

  function getContextualCreatorIdentity() {
    const authorLinks = Array.from(document.querySelectorAll('a[href*="/user/"]'));
    let bestAnchor = null;
    let bestScore = -1;
    for (const anchor of authorLinks) {
      const score = scoreAuthorLink(anchor);
      if (score > bestScore) {
        bestScore = score;
        bestAnchor = anchor;
      }
    }
    if (!bestAnchor || bestScore < 0) {
      return null;
    }
    const name = getAuthorNameFromAnchor(bestAnchor)
      || findNearbyAuthorText(bestAnchor)
      || cleanCreatorName(bestAnchor.textContent || "");
    const href = bestAnchor.getAttribute("href") || "";
    const resolvedUrl = href ? normalizeUrl(new URL(href, window.location.origin).toString()) : "";
    if (!resolvedUrl) {
      return null;
    }
    return {
      url: resolvedUrl,
      name: name || "未命名账号",
    };
  }

  function getCurrentCreatorIdentity() {
    if (isProfilePage()) {
      return {
        url: normalizeUrl(location.href),
        name: getProfilePageCreatorName(),
      };
    }
    const contextual = getContextualCreatorIdentity();
    if (contextual) {
      return contextual;
    }
    return {
      url: normalizeUrl(location.href),
      name: getProfilePageCreatorName(),
    };
  }

  function getCurrentCreatorUrl() {
    return getCurrentCreatorIdentity().url;
  }

  function getCurrentCreatorName() {
    return getCurrentCreatorIdentity().name;
  }

  function cacheKeyForCreator(url) {
    return `${getPlatform()}::${normalizeUrl(url)}`;
  }

  function updateBadge(text, status) {
    let badge = document.getElementById(BADGE_ID);
    if (!badge) {
      badge = document.createElement("div");
      badge.id = BADGE_ID;
      badge.style.cssText = [
        "position:fixed",
        "right:16px",
        "bottom:16px",
        "z-index:999999",
        "padding:8px 12px",
        "border-radius:999px",
        "font-size:13px",
        "font-weight:600",
        "box-shadow:0 10px 24px rgba(0,0,0,.18)",
        "color:#fff",
      ].join(";");
      document.body.appendChild(badge);
    }
    const colors = {
      idle: "#6b7280",
      checking: "#2563eb",
      success: "#16a34a",
      exists: "#0891b2",
      error: "#dc2626",
    };
    badge.style.background = colors[status] || colors.idle;
    badge.textContent = text;
  }

  function renderButton(stateText, disabled) {
    let button = document.getElementById(BUTTON_ID);
    if (!button) {
      button = document.createElement("button");
      button.id = BUTTON_ID;
      button.type = "button";
      button.style.cssText = [
        "position:fixed",
        "right:16px",
        "bottom:64px",
        "z-index:999999",
        "padding:10px 14px",
        "border:none",
        "border-radius:12px",
        "background:#127475",
        "color:#fff",
        "font-size:14px",
        "font-weight:700",
        "cursor:pointer",
        "box-shadow:0 12px 32px rgba(18,116,117,.28)",
      ].join(";");
      button.addEventListener("click", onAddButtonClick);
      document.body.appendChild(button);
    }
    button.textContent = stateText;
    button.disabled = Boolean(disabled);
    button.style.opacity = disabled ? "0.72" : "1";
    button.style.cursor = disabled ? "not-allowed" : "pointer";
  }

  function requestScriptUpsert(payload) {
    const config = readConfig();
    const baseUrl = String(config.baseUrl || "").trim().replace(/\/$/, "");
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: "POST",
        url: `${baseUrl}${PANEL_PATH}`,
        headers: {
          "Content-Type": "application/json",
        },
        data: JSON.stringify({
          password: config.password || "",
          ...payload,
        }),
        onload: (response) => {
          try {
            const data = JSON.parse(response.responseText || "{}");
            if (response.status >= 200 && response.status < 300) {
              resolve(data);
              return;
            }
            reject(new Error(data.detail || data.message || `请求失败(${response.status})`));
          } catch (error) {
            reject(error);
          }
        },
        onerror: () => reject(new Error("无法连接 WebUI，请检查地址和密码配置。")),
      });
    });
  }

  function requestScriptHistory() {
    const config = readConfig();
    const baseUrl = String(config.baseUrl || "").trim().replace(/\/$/, "");
    return new Promise((resolve, reject) => {
      GM_xmlhttpRequest({
        method: "POST",
        url: `${baseUrl}${HISTORY_PATH}`,
        headers: {
          "Content-Type": "application/json",
        },
        data: JSON.stringify({
          password: config.password || "",
        }),
        onload: (response) => {
          try {
            const data = JSON.parse(response.responseText || "{}");
            if (response.status >= 200 && response.status < 300) {
              resolve(data);
              return;
            }
            reject(new Error(data.detail || `请求失败(${response.status})`));
          } catch (error) {
            reject(error);
          }
        },
        onerror: () => reject(new Error("无法连接 WebUI，请检查地址和密码配置。")),
      });
    });
  }

  function getCurrentCreatorPayload() {
    const identity = getCurrentCreatorIdentity();
    return {
      url: identity.url,
      name: identity.name,
    };
  }

  function promptCreatorNameForSubmit(defaultName) {
    const input = window.prompt("确认账号名称", defaultName || "");
    if (input == null) {
      return null;
    }
    const value = String(input || "").trim();
    if (!value) {
      updateBadge("账号名称不能为空，已取消新增。", "error");
      return null;
    }
    return value;
  }

  function showLocalCreatorStatus() {
    const { url, name } = getCurrentCreatorPayload();
    const cacheKey = cacheKeyForCreator(url);
    const cache = readCache();
    const historyMatch = compareCurrentCreatorWithHistory(url);
    if (cache[cacheKey]?.status === "exists" || cache[cacheKey]?.status === "created") {
      renderButton("已新增到 TKWEB", false);
      updateBadge(`缓存状态：${cache[cacheKey].message || "已新增"}`, "exists");
      return;
    }
    if (historyMatch) {
      renderButton("历史已记录", false);
      updateBadge(`历史库匹配：${historyMatch.name || historyMatch.mark || name}`, "exists");
      return;
    }
    renderButton("新增到 TKWEB", false);
    updateBadge("本地不存在", "idle");
  }

  function buildHistoryIndex(items) {
    const byUrl = {};
    for (const item of items || []) {
      const normalizedUrl = normalizeUrl(item.url || "");
      if (!normalizedUrl) {
        continue;
      }
      byUrl[`${item.platform || getPlatform()}::${normalizedUrl}`] = {
        id: item.id || null,
        platform: item.platform || "",
        name: item.name || "",
        mark: item.mark || "",
        url: normalizedUrl,
      };
    }
    return {
      updatedAt: Date.now(),
      byUrl,
    };
  }

  async function syncHistoryFromWebui() {
    const result = await requestScriptHistory();
    const store = buildHistoryIndex(result.items || []);
    saveHistoryStore(store);
    return store;
  }

  function compareCurrentCreatorWithHistory(url) {
    const store = readHistoryStore();
    const key = `${getPlatform()}::${normalizeUrl(url)}`;
    return store.byUrl?.[key] || null;
  }

  function exportHistoryText() {
    const store = readHistoryStore();
    const items = Object.values(store.byUrl || {});
    return items
      .map((item) => `${item.name || item.mark || "未命名账号"}\t${item.url}`)
      .join("\n");
  }

  function downloadTextFile(filename, content) {
    const blob = new Blob([content], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function parseImportText(text) {
    const items = [];
    const seen = new Set();
    for (const line of String(text || "").split(/\r?\n/)) {
      const value = line.trim();
      if (!value) {
        continue;
      }
      const parts = value.split(/\t|\s+/).filter(Boolean);
      const lastPart = parts[parts.length - 1] || "";
      const url = normalizeUrl(lastPart);
      if (!url || seen.has(url)) {
        continue;
      }
      seen.add(url);
      const cutoff = value.lastIndexOf(lastPart);
      const name = (cutoff > 0 ? value.slice(0, cutoff) : "").trim() || "未命名账号";
      items.push({
        id: null,
        platform: url.includes("tiktok.com") ? "tiktok" : "douyin",
        name,
        mark: name,
        url,
      });
    }
    return items;
  }

  function importHistoryText(text) {
    const imported = parseImportText(text);
    const existing = readHistoryStore();
    const merged = {
      updatedAt: Date.now(),
      byUrl: {
        ...(existing.byUrl || {}),
      },
    };
    for (const item of imported) {
      merged.byUrl[`${item.platform}::${item.url}`] = item;
    }
    saveHistoryStore(merged);
    return imported.length;
  }

  async function checkCurrentCreatorStatus() {
    const { url, name } = getCurrentCreatorPayload();
    const cacheKey = cacheKeyForCreator(url);
    const cache = readCache();
    updateBadge("正在向 WebUI 检查当前账号状态...", "checking");
    try {
      const result = await requestScriptUpsert({
        only_check: true,
        url,
        name,
        mark: name,
        platform: getPlatform(),
      });
      cache[cacheKey] = {
        status: result.status,
        message: result.message,
        creatorId: result.creator?.id || null,
        checkedAt: Date.now(),
      };
      saveCache(cache);
      if (result.status === "exists") {
        renderButton("已新增到 TKWEB", false);
        updateBadge(result.message || "账号已存在", "exists");
      } else {
        renderButton("新增到 TKWEB", false);
        updateBadge(result.message || "当前账号尚未新增", "idle");
      }
    } catch (error) {
      updateBadge(`检查失败：${error.message}`, "error");
      showLocalCreatorStatus();
    }
  }

  async function onAddButtonClick() {
    const { url, name } = getCurrentCreatorPayload();
    const confirmedName = promptCreatorNameForSubmit(name);
    if (!confirmedName) {
      renderButton("新增到 TKWEB", false);
      return;
    }
    const cacheKey = cacheKeyForCreator(url);
    const cache = readCache();
    renderButton("提交中...", true);
    updateBadge("正在提交新增请求...", "checking");
    try {
      const result = await requestScriptUpsert({
        only_check: false,
        url,
        name: confirmedName,
        mark: confirmedName,
        platform: getPlatform(),
      });
      cache[cacheKey] = {
        status: result.status,
        message: result.message,
        creatorId: result.creator?.id || null,
        checkedAt: Date.now(),
      };
      saveCache(cache);
      if (result.status === "created" || result.status === "exists") {
        renderButton("已新增到 TKWEB", false);
        updateBadge(result.message, result.status === "created" ? "success" : "exists");
        return;
      }
      renderButton("新增到 TKWEB", false);
      updateBadge(result.message || "新增失败", "error");
    } catch (error) {
      renderButton("新增到 TKWEB", false);
      updateBadge(`新增失败：${error.message}`, "error");
    }
  }

  function openConfigDialog() {
    const current = readConfig();
    const baseUrl = window.prompt("请输入 TKWEB 地址", current.baseUrl || "http://127.0.0.1:5555");
    if (!baseUrl) {
      return;
    }
    const password = window.prompt("请输入 TKWEB 访问密码", current.password || "151150");
    if (!password) {
      return;
    }
    saveConfig({
      baseUrl: baseUrl.trim().replace(/\/$/, ""),
      password: password.trim(),
    });
    updateBadge("配置已保存，当前状态将优先按本地缓存显示。", "success");
    showLocalCreatorStatus();
  }

  async function syncHistoryMenuAction() {
    updateBadge("正在从 WebUI 拉取历史账号...", "checking");
    try {
      const store = await syncHistoryFromWebui();
      updateBadge(`已同步 ${Object.keys(store.byUrl || {}).length} 个账号`, "success");
      showLocalCreatorStatus();
    } catch (error) {
      updateBadge(`同步失败：${error.message}`, "error");
    }
  }

  function exportHistoryMenuAction() {
    const text = exportHistoryText();
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    downloadTextFile(`tkweb-creators-${timestamp}.txt`, text || "");
    updateBadge("账号信息已导出为 TXT 文件。", "success");
  }

  function importHistoryMenuAction() {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".txt,text/plain";
    input.addEventListener("change", async () => {
      const file = input.files?.[0];
      if (!file) {
        return;
      }
      try {
        const text = await file.text();
        const count = importHistoryText(text);
        updateBadge(`已从 TXT 导入 ${count} 条账号信息`, count > 0 ? "success" : "idle");
        showLocalCreatorStatus();
      } catch (error) {
        updateBadge(`导入失败：${error.message}`, "error");
      }
    }, { once: true });
    input.click();
  }

  function checkCurrentCreatorStatusMenuAction() {
    checkCurrentCreatorStatus().catch(() => {});
  }

  function bootstrap() {
    GM_registerMenuCommand("配置 TKWEB 地址和密码", openConfigDialog);
    GM_registerMenuCommand("从 WebUI 同步历史账号", syncHistoryMenuAction);
    GM_registerMenuCommand("检查当前账号状态", checkCurrentCreatorStatusMenuAction);
    GM_registerMenuCommand("导出账号信息", exportHistoryMenuAction);
    GM_registerMenuCommand("导入账号信息", importHistoryMenuAction);
    renderButton("新增到 TKWEB", false);
    updateBadge("脚本已加载，优先使用本地缓存和历史库判断状态。", "idle");
    showLocalCreatorStatus();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap, { once: true });
  } else {
    bootstrap();
  }
})();
