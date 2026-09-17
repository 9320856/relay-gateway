# 贡献与 Git 提交流程

## 本地开发

    go test -count=2 ./...
    go test -cover ./...
    go vet ./...
    node web\app_runtime_test.mjs
    go build -o relay-gateway.exe .

GitHub Actions 会在 Linux 上执行重复测试、覆盖率、go vet 和 race detector。修改路由、鉴权、异步任务、SQLite 或适配器时，请优先补对应的回归测试；前端运行时逻辑修改请同步更新 web/app_runtime_test.mjs。

## 修改边界

- router/ 负责 HTTP 输入输出和安全边界，不要在 handler 中绕过任务映射直接根据用户参数选择上游。
- service/ 负责候选渠道、优先级、权重、熔断和重试策略。
- adapter/ 负责上游协议差异；不要把某个供应商的字段假设泄漏到通用模型结构。
- db/ 的 schema 变更必须考虑已有 SQLite 数据库和启动时 schema marker 校验。
- audit/ 中任何新增日志字段都要先判断是否包含 API Key、Cookie、Token、签名或密码。
- web/ 的页面资源会被 go:embed 编译进二进制，前端改动需要重新构建验证。

## 提交前检查

    git status --short
    git diff --check
    git ls-files | Select-String -Pattern 'config\.yaml|\.db$|\.exe$|\.env$'
    go test -count=2 ./...
    go vet ./...

生产配置、数据库、WAL、日志、编译产物和异步恢复日志已加入 .gitignore。config.yaml.example 可以提交；config.yaml 只能作为本地运行时文件。提交前如果发现真实密钥曾经进入 Git 历史，应立即撤销/轮换密钥，并使用专门的历史清理流程，不要只依赖新的 .gitignore。

## Commit 建议

使用简短、可检索的 Conventional Commits 风格：

- feat(router): ...
- fix(video): ...
- docs: ...
- test: ...
- chore: ...

一次提交尽量只围绕一个可说明的变化；涉及多个相互依赖的源码、测试和文档文件时可以放在同一个提交中。

## 提交到 GitHub

当前仓库可以先在本地整理和验证，再绑定远程仓库。远程地址需要替换成实际仓库：

    git remote add origin https://github.com/<owner>/<repository>.git
    git branch -M main
    git add -A
    git commit -m "docs: prepare relay gateway for GitHub"
    git push -u origin main

如果仓库已有 origin，使用 git remote -v 检查地址后再 push；不要把 Gateway Key、上游 API Key、管理员密码或 gateway.db 放入提交。
