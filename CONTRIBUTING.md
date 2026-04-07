# Contributing to rune

感谢你对 rune 的贡献兴趣！

## 开发环境准备

1. Fork 并 clone 本仓库
2. 安装 Rust 开发工具链
3. 按照 README 中的说明完成本地环境搭建

## 分支命名规范

从 `dev` 分支创建你的工作分支：

- `feature/<描述>` — 新功能
- `fix/<描述>` — Bug 修复
- `chore/<描述>` — 构建、CI、文档等杂项

## Commit 规范

遵循 [Conventional Commits](https://www.conventionalcommits.org/)：

```
<type>(<scope>): <description>

[optional body]
```

常用 type：`feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

## PR 流程

1. 从 `dev` 创建功能分支
2. 完成开发，确保测试通过
3. 提交 PR 到 `dev` 分支
4. 等待 Code Review
5. Review 通过后合并

## 代码规范

- 提交前确保 CI 检查全部通过
- 新功能需附带测试
- 保持代码风格与项目一致
