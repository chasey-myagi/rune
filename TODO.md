# Rune TODO

## P0 — 部署前必须有
- [ ] **认证机制**：Caster 注册鉴权 + API 调用鉴权（公网暴露必须有）
- [ ] **HTTPS**：前置 Caddy 反代，自动 TLS

## P1 — 部署后尽快补
- [ ] **Claude Code Skill 集成**：通过 Skill + 脚本发现已注册的 Caster，再通过脚本调度使用 Caster（不走 MCP，走 Skill 脚本更灵活）
- [ ] **Dashboard**：简单 Web 页面，展示在线 Caster、可用 Rune、最近调用记录
- [ ] **持久化**：异步任务结果用 SQLite 存储，重启不丢

## P2 — 用起来之后再说
- [ ] 调用日志 & 指标
- [ ] Rate limiting
- [ ] Rune 版本管理（灰度切换）
- [ ] chasey.ai 计费层接入
