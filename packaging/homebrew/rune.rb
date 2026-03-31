# typed: false
# frozen_string_literal: true

class Rune < Formula
  desc "Define functions, get APIs + workflows + distributed execution"
  homepage "https://github.com/chasey-myagi/rune"
  version "@@VERSION@@"
  license :cannot_represent

  on_macos do
    on_arm do
      url "https://github.com/chasey-myagi/rune/releases/download/v@@VERSION@@/rune-v@@VERSION@@-darwin-arm64.tar.gz"
      sha256 "@@SHA256_DARWIN_ARM64@@"
    end
    on_intel do
      url "https://github.com/chasey-myagi/rune/releases/download/v@@VERSION@@/rune-v@@VERSION@@-darwin-amd64.tar.gz"
      sha256 "@@SHA256_DARWIN_AMD64@@"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/chasey-myagi/rune/releases/download/v@@VERSION@@/rune-v@@VERSION@@-linux-arm64.tar.gz"
      sha256 "@@SHA256_LINUX_ARM64@@"
    end
    on_intel do
      url "https://github.com/chasey-myagi/rune/releases/download/v@@VERSION@@/rune-v@@VERSION@@-linux-amd64.tar.gz"
      sha256 "@@SHA256_LINUX_AMD64@@"
    end
  end

  def install
    bin.install "rune"
    bin.install "rune-server"
  end

  def caveats
    <<~EOS
      Rune CLI and Runtime have been installed.

      Quick start:
        # Start the runtime server (HTTP :50060, gRPC :50070)
        rune-server

        # Check CLI version
        rune --version

      Configuration via environment variables:
        RUNE_SERVER__HTTP_HOST  (default: 0.0.0.0)
        RUNE_SERVER__HTTP_PORT  (default: 50060)
        RUNE_SERVER__GRPC_PORT  (default: 50070)
        RUNE_LOG__LEVEL         (default: info)

      Documentation: https://github.com/chasey-myagi/rune
    EOS
  end

  test do
    system "#{bin}/rune", "--version"
  end
end
