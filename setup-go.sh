#!/usr/bin/env bash
# 一键配置 Go 环境（Linux）。在全新云主机上跑一次即可编译本项目。
#
#   ./setup-go.sh                # 安装默认版本
#   GO_VERSION=1.23.4 ./setup-go.sh   # 指定版本
#
# 做的事：
#   1. 已有满足版本要求(>=1.22)的 go 则直接跳过安装
#   2. 从国内镜像 golang.google.cn 下载官方 tar 包（云主机访问 go.dev 常超时）
#   3. 解压到 /usr/local/go，并写 /etc/profile.d/go.sh 配置 PATH
#   4. 设置 GOPROXY=goproxy.cn（拉依赖不翻墙）
set -euo pipefail

GO_VERSION="${GO_VERSION:-1.22.10}"
MIN_MINOR=22   # go.mod 要求 go 1.22.0

# root 检查（写 /usr/local 和 /etc/profile.d 需要）
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  command -v sudo >/dev/null || { echo "请用 root 运行，或安装 sudo"; exit 1; }
  SUDO=sudo
fi

# 已有满足要求的 go 就跳过
if command -v go >/dev/null 2>&1; then
  installed=$(go version | awk '{print $3}' | sed 's/^go//')   # 如 1.22.10
  minor=$(echo "$installed" | cut -d. -f2)
  if [ "$minor" -ge "$MIN_MINOR" ]; then
    echo "已安装 go $installed（满足 >=1.$MIN_MINOR），跳过安装"
  else
    echo "已安装 go $installed 过旧，升级到 $GO_VERSION"
    command -v go | grep -q /usr/local/go && $SUDO rm -rf /usr/local/go
  fi
fi

if ! command -v go >/dev/null 2>&1 || [ ! -d /usr/local/go ]; then
  case "$(uname -m)" in
    x86_64)  arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) echo "不支持的架构: $(uname -m)"; exit 1 ;;
  esac

  tarball="go${GO_VERSION}.linux-${arch}.tar.gz"
  url="https://golang.google.cn/dl/${tarball}"
  echo "==> 下载 $url"
  curl -fL --retry 3 -o "/tmp/${tarball}" "$url" \
    || wget -O "/tmp/${tarball}" "$url"

  echo "==> 解压到 /usr/local/go"
  $SUDO rm -rf /usr/local/go
  $SUDO tar -C /usr/local -xzf "/tmp/${tarball}"
  rm -f "/tmp/${tarball}"
fi

# PATH（所有用户、所有后续登录会话生效）
if [ ! -f /etc/profile.d/go.sh ]; then
  echo "==> 写入 /etc/profile.d/go.sh"
  echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' | $SUDO tee /etc/profile.d/go.sh >/dev/null
fi
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin

# 国内代理，拉依赖不超时
go env -w GOPROXY=https://goproxy.cn,direct

echo
echo "==> 完成: $(go version)"
echo "    GOPROXY=$(go env GOPROXY)"
echo
echo "当前 shell 直接可用；新开的 shell 自动生效（来自 /etc/profile.d/go.sh）。"
echo "验证编译: cd $(dirname "$0") && go build ./..."
