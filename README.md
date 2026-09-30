# Relay Gateway

一个本地优先的 AI 聚合网关。它将多个上游渠道统一为 OpenAI 与 Anthropic 兼容接口，并提供渠道配置、快速测试、异步任务、媒体资产和调用日志控制台。

> **V0.2.2 开发测试版，非正式发布。** 项目仍在快速迭代，后续可能进行较大范围的代码、配置、数据结构和接口重构。请仅在测试环境使用；升级前备份 SQLite 数据库、媒体目录和 `RELAY_DB_ENCRYPTION_KEY`，不要把它当作稳定生产版本。

## 功能

- 接入 `openai`、`anthropic`、`newapi`、`sub2api` 渠道，按模型映射、优先级、权重和熔断状态调度。
- 提供 Chat Completions、Responses、Anthropic Messages、Embeddings、图片、语音和异步视频接口，支持流式响应与模型列表。
- 通过 Protocol Profile 管理上游请求与响应转换；渠道可逐步迁移到 Profile。
- 在控制台快速测试对话、图片和视频请求，查看结构化日志、异步任务与托管媒体。
- 使用 SQLite 保存渠道、审计记录和任务元数据；上游密钥可通过环境变量提供的加密密钥加密保存。

渠道中的「获取模型」会展示可勾选的模型列表，新渠道首次获取默认全选。可以搜索、取消勾选、全选或清空，再点击「保存渠道」；该渠道只接受已选模型及其映射别名的调用。编辑已有渠道时，也可以取消模型并保存。全部取消后，渠道不再接受新的模型调用，已经提交的异步任务仍可继续查询和完成。

自动同步只更新候选列表，不会启用新模型或重新启用已取消的模型。再次编辑时可重新勾选。升级前已有渠道保持原调用行为，首次保存模型选择后开始按已选名单限制。管理接口 `POST /api/channels` 使用 `selected_models` 字符串数组保存选择，`[]` 表示全部取消，省略该字段会保留原选择；配置了选择的渠道响应也会返回此字段。

## V0.2.2 更新

本次修复了代码审查发现的 9 项请求执行、并发调度和任务持久化问题，并补充对应回归测试。

- **请求执行与凭据保护**：Legacy、Profile 和模型发现共用有界连接池及重定向策略，阻止自动转发凭据和重放提交；流式、JSON 和普通响应共用读取/写入错误分类。
- **调度与熔断**：按候选渠道和权重独立轮询，修复混合模型流量中的渠道饥饿；上游故障计入熔断；半开探测带周期和所有权校验，旧请求不会释放新探测。
- **任务与媒体**：后台仅领取已接受且具有上游任务 ID 的任务；幂等重放复用公开媒体投影；多图先登记完整资产集合再保存，部分失败时保持待完成，重试复用已保存资产。
- **Profile 版本**：新建草稿只插入，重复版本返回 HTTP 409；自动编号与插入在同一事务中完成，并处理操作取消后的回滚。
- **SQLite 连接**：连接因寿命到期或失效而替换时，重新应用一致的数据库设置；不兼容数据库仍在初始化设置前被拒绝。

上游提交、查询和模型发现请求不自动跟随重定向，请将渠道 Base URL 配置为最终 API 地址。推理或任务创建发生不确定失败时，当前请求不会被自动重放；后续请求会根据熔断状态选择渠道。客户端取消和响应写入失败不计入上游故障。

创建 Protocol Profile 版本时，省略版本号会原子分配下一编号；编辑已有草稿使用版本更新接口。启用 `required` 媒体保存后，整批资产可用才会返回完成结果；幂等重放和状态查询均按当前请求域名返回托管媒体链接，保存未完成时返回 `materializing`。

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

已有数据库应使用最初配置的 `RELAY_DB_ENCRYPTION_KEY`。缺少密钥或密钥不匹配时，服务无法解密已保存的渠道凭据，会拒绝启动。新终端或后台启动进程也需要加载原密钥，以及已配置的媒体白名单等环境变量。

服务启动后可检查健康状态，正常时返回 `status: ok`：

```powershell
Invoke-RestMethod http://localhost:8000/health
```

首次访问服务的 IP 或域名（例如 `http://<服务器IP>:8000/` 或 `https://<网关域名>/`）即可创建管理员，本机也可访问 [http://localhost:8000/setup](http://localhost:8000/setup)。默认不限制首次初始化的来源 IP 或域名。初始化时生成的网关 API Key 仅显示一次，请立即保存。随后登录 `/login` 添加渠道、同步模型并配置映射。

若需要限制谁能创建首个管理员，可在启动前自行设置 `RELAY_SETUP_SECRET`（系统不会自动生成）：

```powershell
$env:RELAY_SETUP_SECRET = "请替换为高熵初始化口令"
go run .
```

设置后，初始化页面会显示必填的「初始化口令」输入框。页面仅通过 `X-Relay-Setup-Secret` 请求头提交该值；直接调用 `/api/auth/setup` 也必须提供此请求头。未设置时无需额外口令；已有管理员后，初始化接口返回 HTTP 409。

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

### 根路径反向代理

通过根路径（`/`）部署时，若实际连接来自回环或私有 IPv4/IPv6 地址，媒体链接会自动读取反代传来的外部域名、协议和端口，无需设置 `RELAY_TRUST_PROXY`。主机名采用有效的 `X-Forwarded-Host` 首项，否则使用 `Host`；实际 TLS 连接使用 HTTPS，否则采用有效的 `X-Forwarded-Proto` 首项，缺失时使用 HTTP。

`RELAY_TRUST_PROXY=1` 可显式信任转发头；设为 `0` 时媒体链接忽略转发头。自动识别仅用于媒体链接，管理认证和 Cookie 仍按显式信任策略处理（包括通过转发协议判断是否设置 Secure Cookie）。首次初始化与 `RELAY_TRUST_PROXY` 独立，无需启用代理信任即可通过 IP 或域名创建管理员。公网来源或 CDN 不在媒体链接的自动信任范围内。

域名不会持久化，历史媒体链接会在每次响应时按当前请求重新生成。

在配置了证书的 HTTPS `server` 块中，可使用如下 Nginx 配置：

```nginx
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Host $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
}
```

保留 `Host` 可匹配现有 CSRF 来源校验；覆盖转发头可确保其值来自该代理。`proxy_buffering off` 有助于流式请求。常见的本机代理拓扑无需额外设置应用环境变量即可生成正确的媒体链接，但应限制后端端口仅供代理访问。若代理到应用使用明文 HTTP，应用无法从连接本身得知外部 HTTPS；需由代理正确传入 `X-Forwarded-Proto`。相关指令见 [Nginx `proxy_set_header` 文档](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_set_header)。

### Fake-IP 网络中的媒体下载

媒体下载默认检查并固定连接到解析后的公网 IP。使用 Mihomo 等 Fake-IP 网络时，若媒体域名返回 `198.18.0.0/15` 或 `2001:2::/48` 地址，优先为该准确域名配置真实 DNS 解析。网关已对 Profile 渠道同一注册域下的媒体提供受限 Fake-IP 支持。这两个基准测试网段不是普通公网地址；IPv6 范围见 [IANA 特殊用途地址登记](https://www.iana.org/assignments/iana-ipv6-special-registry)。

若渠道将图片或视频保存在不同注册域的 CDN，部署者可在启动前配置准确媒体域名白名单：

```powershell
$env:RELAY_MEDIA_TRUSTED_FAKE_IP_HOSTS = "cdn.provider.com,images.provider.net"
.\relay-gateway.exe
```

白名单只接受准确域名，不接受 URL、IP 或通配符；不会根据上游响应自动添加域名。该设置同时适用于同步和后台下载，只额外允许这些域名的 Fake-IP DNS 结果，仍拒绝其他私网地址和公网/Fake-IP 混合结果，并逐跳检查重定向。正常公网解析始终允许。Fake-IP 下载要求本机代理能够路由这些地址；此设置不改变系统代理配置或媒体公开链接的反代规则。

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
- 登录、初始化和修改密码共用独立的密码计算预算，默认最多同时处理 2 个请求，超出时返回 HTTP 429（`Retry-After: 1`），请稍后重试。启动前可设置 `RELAY_AUTH_HASH_CONCURRENCY=1..32`；每个 Argon2id 计算约使用 64 MiB 内存，应按服务器资源配置。登录限流同时计入正在校验的请求。
- 首次设置默认允许任意来源 IP 或域名创建首个管理员；可选的 `RELAY_SETUP_SECRET` 要求调用者提供 `X-Relay-Setup-Secret`，网页提供专用输入框。此限制与 `RELAY_TRUST_PROXY` 独立；生产环境需使用 TLS 并限制管理端口。
- 忘记管理员凭据可在本机运行 `go run . admin reset`，按提示输入 `RESET`。该操作会移除管理员与管理 Session，保留渠道和日志。
- `config.yaml`、SQLite 数据库及 WAL、媒体文件、日志和编译产物都是本地运行数据，已被忽略，不应提交。请将数据库、媒体目录和加密密钥一起纳入备份与恢复方案。

异步任务和媒体元数据保存在 SQLite，媒体默认位于数据库同级目录的 `media/` 下。面向外部的媒体链接具有访问能力；不要将其作为长期密钥，也不要分享给无关人员。

删除媒体会立即撤销链接，并在后台清理文件；文件被占用或存储错误时保留删除任务自动重试。共享文件会等最后一个有效引用撤销后再删除。任务路由的内存缓存最多保留 10,000 条，并每分钟清理超过 7 天的条目；淘汰后仍可从 SQLite 读取历史映射。

## 开发

```powershell
go test -count=2 ./...
go test -cover ./...
go vet ./...
node web/app_runtime_test.mjs
node web/media_runtime_test.mjs
node web/clipboard_runtime_test.mjs
```

提交前还应运行：

```powershell
git status --short
git diff --check
```

详细开发边界和提交要求参见 [CONTRIBUTING.md](CONTRIBUTING.md)。

GitHub Actions 在 Linux 上执行重复测试、覆盖率、`go vet` 和 `go test -race ./...`。本地运行 race 检测需要启用 CGO 并安装可用的 C 编译器。

## 许可证

[MIT License](LICENSE)。使用上游 API、模型服务、生成内容和渠道凭据时，也请遵守相应服务商条款及适用法律。
