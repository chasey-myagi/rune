# Homebrew Formula for Rune

## 安装

```bash
brew tap chasey-myagi/tap
brew install rune
```

## Formula 说明

`rune.rb` 会安装两个二进制：

- `rune` -- CLI 工具
- `rune-server` -- Runtime 服务

支持平台：macOS (arm64, x86_64)、Linux (arm64, x86_64)。

## 发版更新流程

Formula 使用占位符，CI 发版时自动替换：

| 占位符 | 含义 |
|--------|------|
| `@@VERSION@@` | 版本号（不带 `v` 前缀） |
| `@@SHA256_DARWIN_ARM64@@` | macOS ARM64 tarball 的 sha256 |
| `@@SHA256_DARWIN_X86_64@@` | macOS x86_64 tarball 的 sha256 |
| `@@SHA256_LINUX_ARM64@@` | Linux ARM64 tarball 的 sha256 |
| `@@SHA256_LINUX_X86_64@@` | Linux x86_64 tarball 的 sha256 |

CI 脚本示例（在 release workflow 中）：

```bash
VERSION="0.1.0"

# 下载各平台 tarball 并计算 sha256
SHA_DARWIN_ARM64=$(shasum -a 256 rune-v${VERSION}-darwin-arm64.tar.gz | awk '{print $1}')
SHA_DARWIN_X86_64=$(shasum -a 256 rune-v${VERSION}-darwin-x86_64.tar.gz | awk '{print $1}')
SHA_LINUX_ARM64=$(shasum -a 256 rune-v${VERSION}-linux-arm64.tar.gz | awk '{print $1}')
SHA_LINUX_X86_64=$(shasum -a 256 rune-v${VERSION}-linux-x86_64.tar.gz | awk '{print $1}')

# 替换占位符
sed -e "s/@@VERSION@@/${VERSION}/g" \
    -e "s/@@SHA256_DARWIN_ARM64@@/${SHA_DARWIN_ARM64}/g" \
    -e "s/@@SHA256_DARWIN_X86_64@@/${SHA_DARWIN_X86_64}/g" \
    -e "s/@@SHA256_LINUX_ARM64@@/${SHA_LINUX_ARM64}/g" \
    -e "s/@@SHA256_LINUX_X86_64@@/${SHA_LINUX_X86_64}/g" \
    packaging/homebrew/rune.rb > Formula/rune.rb

# 提交到 homebrew-tap 仓库
```

## Tap 仓库结构

`chasey-myagi/homebrew-tap` 仓库的目录结构：

```
homebrew-tap/
├── Formula/
│   └── rune.rb    # 由 CI 从本模板生成
└── README.md
```
