# Docker WebUI 部署说明

当前仓库构建出的镜像默认启动 `WebUI`。

## 构建镜像

```bash
docker build -t tiktok-downloader-webui .
```

## 运行容器

```bash
docker run -d \
  --name tkdown-webui \
  -p 5555:5555 \
  -v /your/path/Volume:/app/Volume \
  -v /your/path/webui-data:/app/data \
  tiktok-downloader-webui
```

## 目录说明

- `/app/Volume`：引擎配置、下载内容、旧项目数据目录
- `/app/data`：WebUI 面板数据、任务日志、运行时状态目录

## 访问地址

- WebUI：`http://127.0.0.1:5555/`
- 引擎 API 文档：`http://127.0.0.1:5555/api/docs`

## 说明

- 如果你使用的是 fork 仓库，请在同步 `Dockerfile`、`.dockerignore` 和 WebUI 相关代码后重新构建镜像。
- Docker 容器不能直接访问宿主机桌面环境，所以 `从浏览器读取 Cookie` 一类依赖本地 GUI 的功能可能不可用。
- 如果你只想运行终端模式或 API 模式，可以自行覆盖容器启动命令。
