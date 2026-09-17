# Relay Gateway

一个本地优先的 AI 聚合网关。它将多个上游渠道统一为 OpenAI 与 Anthropic 兼容接口，并提供渠道配置、快速测试、异步任务、媒体资产和调用日志控制台。

> **V0.1 开发测试版，非正式发布。** 项目仍在快速迭代，后续可能进行较大范围的代码、配置、数据结构和接口重构。请仅在测试环境使用；升级前备份 SQLite 数据库、媒体目录和 `RELAY_DB_ENCRYPTION_KEY`，不要把它当作稳定生产版本。

## 功能

- 接入 `openai`、`anthropic`、`newapi`、`sub2api` 渠道，按模型映射、优先级、权重和熔断状态调度。
- 提供 Chat Completions、Responses、Anthropic Messages、Embeddings、图片、语音和异步视频接口，支持流式响应与模型列表。
- 通过 Protocol Profile 管理上游请求与响应转换；渠道可逐步迁移到 Profile。
- 在控制台快速测试对话、图片和视频请求，查看结构化日志、异步任务与托管媒体。
- 使用 SQLite 保存渠道、审计记录和任务元数据；上游密钥可通过环境变量提供的加密密钥加密保存。

## 快速开始

### 前置条件

- Go 1.26 或更高版本
- 一个可访问的上游渠道

复制配置模板：

```powershell
Copy-Item config.yaml.example config.yaml
```

默认配置如下；渠道、上游密钥和管理员凭据在控制台中保存到 SQLite：

```yaml
port: 8000
database_path: gateway.db
```

设置一个稳定、高熵的数据库加密密钥，再启动服务。未设置该变量时，保存渠道凭据会被拒绝，避免以明文写入数据库：

```powershell
$env:RELAY_DB_ENCRYPTION_KEY = "请替换为稳定的高熵随机值"
go run .
```

首次从本机访问 [http://localhost:8000/setup](http://localhost:8000/setup) 创建管理员。初始化时生成的网关 API Key 仅显示一次，请立即保存。登录 [http://localhost:8000/login](http://localhost:8000/login) 后添加渠道、同步模型并配置映射。

客户端使用 `http://localhost:8000/v1` 作为 API Base URL：

```bash
curl http://localhost:8000/v1/models \
  -H "Authorization: Bearer <GATEWAY_KEY>"
```

Anthropic 客户端调用 `/v1/messages`。前端页面由 Go `embed` 编译进二进制；修改 `web/` 或源码后应重新构建并重启：

```powershell
go build -o relay-gateway.exe .
.\relay-gateway.exe
```

## 接口

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/v1/models` | GET | 列出可用模型 |
| `/v1/chat/completions`、`/v1/responses`、`/v1/messages` | POST | 对话与兼容协议 |
| `/v1/embeddings`、`/v1/audio/speech` | POST | Embedding 与语音 |
| `/v1/images/generations`、`/v1/images/edits` | POST | 图片生成与编辑 |
| `/v1/images/jobs`、`/v1/images/jobs/{id}` | POST、GET | 异步图片任务 |
| `/v1/videos`、`/v1/videos/{id}` | POST、GET | 异步视频任务 |
| `/v1/videos/{id}/content` | GET、HEAD | 已登记视频内容 |
| `/health` | GET | 数据库健康状态 |

提交异步视频后保存返回的 `id` 或 `task_id`，再轮询状态：

```bash
curl -X POST http://localhost:8000/v1/videos \
  -H "Authorization: Bearer <GATEWAY_KEY>" \
  -H "Content-Type: application/json" \
  -d @examples/video-request.json

curl http://localhost:8000/v1/videos/<id> \
  -H "Authorization: Bearer <GATEWAY_KEY>"
```

## 安全与运行数据

- 控制台与 `/api` 管理接口需要管理员会话；写请求还需要 `X-CSRF-Token`。
- `/v1` 使用独立网关 Key（`Authorization: Bearer` 或 `x-api-key`），管理员 Cookie 不能替代它。
- 管理 Session 使用 `HttpOnly` 与 `SameSite=Strict` Cookie，管理员密码使用 Argon2id。
- 首次设置默认仅允许本机访问。通过受信任反向代理初始化时，设置 `RELAY_TRUST_PROXY=1` 与 `RELAY_SETUP_SECRET`，并传入 `X-Relay-Setup-Secret`；生产环境需使用 TLS 并限制管理端口。
- 忘记管理员凭据可在本机运行 `go run . admin reset`，按提示输入 `RESET`。该操作会移除管理员与管理 Session，保留渠道和日志。
- `config.yaml`、SQLite 数据库及 WAL、媒体文件、日志和编译产物都是本地运行数据，已被忽略，不应提交。请将数据库、媒体目录和加密密钥一起纳入备份与恢复方案。

异步任务和媒体元数据保存在 SQLite，媒体默认位于数据库同级目录的 `media/` 下。面向外部的媒体链接具有访问能力；不要将其作为长期密钥，也不要分享给无关人员。

## 开发

```powershell
go test -count=2 ./...
go vet ./...
node web/app_runtime_test.mjs
node web/media_runtime_test.mjs
```

提交前还应运行：

```powershell
git status --short
git diff --check
```

详细开发边界和提交要求参见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可证

[MIT License](LICENSE)。使用上游 API、模型服务、生成内容和渠道凭据时，也请遵守相应服务商条款及适用法律。
