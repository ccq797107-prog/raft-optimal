# CD-Raft 真实环境部署与测试指南

本文档说明如何在**真实机器 / 真实网络（含弹性 IP）**上部署 CD-Raft 集群、启动节点、并用客户端发送读写请求。所有内容对应当前代码（`cmd/cdraft`、`cmd/cdraft-client`、`internal/topology`、`internal/cdraft`），不是泛泛而谈。

---

## 1. 组件与端口模型（先理解这个，否则配 IP 会踩坑）

每个节点是**一个进程**，启动时在**两个 TCP 端口**上提供同一套 gRPC 服务：

| 配置字段 | 用途 | 谁会连它 |
|---|---|---|
| `listenAddress` | 客户端请求 + **同域**节点之间的内部 RPC | 客户端、同域其它节点 |
| `interDomainAddress` | **跨域**节点之间的内部 RPC（默认的绑定 + 拨号地址） | 其它域的节点 |

为支持云上弹性 IP（bind 地址 ≠ 对外宣告地址），还有三个**可选**字段（不填则行为与以前完全一致）：

| 可选字段 | 用途 | 默认值 |
|---|---|---|
| `bindListenAddress` | 客户端/同域服务器本机 `net.Listen` 绑定地址 | `listenAddress` |
| `bindInterDomainAddress` | 跨域服务器本机 `net.Listen` 绑定地址 | `interDomainAddress` |
| `publicInterDomainAddress` | **跨域对端拨号用的弹性/公网 IP:port**（弹性 IP 变了只改这个） | `interDomainAddress` |

节点之间互相拨号的规则（见 `internal/cdraft/rpc_node.go` 的 `peerAddress`）：

- 目标是**同域**节点 → 拨它的 `listenAddress`
- 目标是**跨域**节点 → 拨它的 `publicInterDomainAddress`（未配置则回退 `interDomainAddress`）
- **客户端** → 拨任意节点的 `listenAddress`（不是 GL 也没关系，会自动重定向到 GL）

本机 `net.Listen` 绑定（见 `Start()`）：客户端服务器绑 `bindListenAddress`（默认 `listenAddress`），跨域服务器绑 `bindInterDomainAddress`（默认 `interDomainAddress`）。

> 关键点：**bind 地址和对外宣告地址现在可以分离**。弹性 IP 是网关处的 NAT 映射、无法在本机直接 `net.Listen` 绑定，因此云上的做法是：`bindInterDomainAddress` 绑 `0.0.0.0:port`（本机可绑），`publicInterDomainAddress` 填弹性 IP（供跨域对端拨号）。详见第 3 节。

集群规则（`internal/topology/config.go` 的 `Validate`）：
- 至少 **2 个域**；生产容错建议 **3 个域 × 每域 3 节点**（域内多数派 = 2/3，全局重选需要 `N-1` 个 Domain Leader）。
- `activeMigrationEnabled=true` 必须同时 `optimizerEnabled=true`。
- 真实环境必须 `networkSimulation.enabled=false`（延迟由真实网络提供，不要再人为注入）。

---

## 2. 前置准备与构建

每台机器都需要能运行二进制。推荐在一台机器上交叉编译，再分发：

```bash
# 在项目根目录
# 本机架构构建
go build -o bin/cdraft        ./cmd/cdraft
go build -o bin/cdraft-client ./cmd/cdraft-client

# 如果目标机是 linux/amd64（常见云主机）
GOOS=linux GOARCH=amd64 go build -o bin/cdraft-linux-amd64        ./cmd/cdraft
GOOS=linux GOARCH=amd64 go build -o bin/cdraft-client-linux-amd64 ./cmd/cdraft-client
```

把二进制 + 配置文件 `scp` 到每台机器即可，无需在目标机装 Go。

---

## 3. 网络与地址模型（弹性 IP 重点）

云上的「弹性 IP / 公网 IP」通常是 **NAT 映射**：实例网卡上其实只有**私网 IP**，弹性 IP 在网关处被 NAT 到这个私网 IP。也就是说，进程**无法直接 `net.Listen("tcp", "<弹性IP>:port")`**（会报 `cannot assign requested address`），因为弹性 IP 不在网卡上。

而本项目当前 bind 地址 == 对外宣告地址（同一字段），所以请按下面三种方式之一选型：

### 方式 A（最简单，强烈推荐）：同一内网 / VPC，用私网 IP
所有节点在同一个 VPC（或 VPC 对等连接、专线）内，配置里直接写**私网 IP**。私网 IP 在网卡上可绑定，节点之间内网互通，零额外组件。

### 方式 B（跨地域 / 跨公网，推荐）：叠加网络（WireGuard / Tailscale）
跨地域、需要走公网时，用 WireGuard 或 Tailscale 给每台机器一个**稳定的虚拟 IP**（例如 `100.64.x.x`）。这个虚拟 IP **既能在本机绑定，又能被所有对端路由**，于是配置里填这些虚拟 IP 即可，彻底绕开 NAT，而且自带加密（本项目 gRPC 默认明文，见第 9 节）。这是跨地域跑分布式共识最干净的做法。

### 方式 C（用裸弹性 IP 跨公网）：bind≠advertise，纯配置实现（已支持）
跨地域、对端必须通过弹性 IP 访问、而本机只能绑私网 IP 时，bind 地址和宣告地址不一样。现在**不需要改代码**，直接用配置里的可选字段即可（见第 1 节）：

- `bindInterDomainAddress`: `"0.0.0.0:8101"`（本机可绑，接收 NAT 进来的弹性 IP 流量）
- `publicInterDomainAddress`: `"<该节点的弹性IP>:8101"`（供其它域的节点拨号）
- `interDomainAddress`: 仍填私网 IP:port（作为缺省/同 VPC 回退）

**域↔区域/VPC 映射的核心思路**：一个 `domainCode` 对应一个云上区域/VPC。同域节点走私网（`listenAddress`/`interDomainAddress` 用私网 IP，零公网开销）；跨域节点走弹性 IP（`publicInterDomainAddress`）。

> 运维收益：**弹性 IP 变更时，只需在 `cluster.json` 里改对应节点的 `publicInterDomainAddress`，重启该节点即可，其它什么都不用动。** 完整示例见 `config/cluster.cloud.example.json`。

> 简而言之：同 VPC 用私网（方式 A），跨地域要么叠加网络（方式 B，自带加密），要么裸弹性 IP + 上面的可选字段（方式 C）。

---

## 4. 编写真实拓扑配置

复制 `config/cluster.json` 改成你的真实地址。下面是一个 **3 域 × 3 节点** 的例子（方式 A/B：这里的 IP 都是“可绑定且对端可达”的地址，比如各 VPC 私网 IP 或 WireGuard 虚拟 IP）：

```json
{
  "applicationGroup": "cd-raft",
  "nodes": [
    {"id": "a1", "domainCode": "domain-a", "listenAddress": "10.0.1.11:7101", "interDomainAddress": "10.0.1.11:8101"},
    {"id": "a2", "domainCode": "domain-a", "listenAddress": "10.0.1.12:7101", "interDomainAddress": "10.0.1.12:8101"},
    {"id": "a3", "domainCode": "domain-a", "listenAddress": "10.0.1.13:7101", "interDomainAddress": "10.0.1.13:8101"},

    {"id": "b1", "domainCode": "domain-b", "listenAddress": "10.0.2.11:7101", "interDomainAddress": "10.0.2.11:8101"},
    {"id": "b2", "domainCode": "domain-b", "listenAddress": "10.0.2.12:7101", "interDomainAddress": "10.0.2.12:8101"},
    {"id": "b3", "domainCode": "domain-b", "listenAddress": "10.0.2.13:7101", "interDomainAddress": "10.0.2.13:8101"},

    {"id": "c1", "domainCode": "domain-c", "listenAddress": "10.0.3.11:7101", "interDomainAddress": "10.0.3.11:8101"},
    {"id": "c2", "domainCode": "domain-c", "listenAddress": "10.0.3.12:7101", "interDomainAddress": "10.0.3.12:8101"},
    {"id": "c3", "domainCode": "domain-c", "listenAddress": "10.0.3.13:7101", "interDomainAddress": "10.0.3.13:8101"}
  ],
  "features": {
    "fastReturnEnabled": true,
    "optimizerEnabled": true,
    "activeMigrationEnabled": true
  },
  "networkSimulation": { "enabled": false }
}
```

要点：
- **同一份 `cluster.json` 分发到所有节点**，每个节点用 `-node` 指定自己是谁。
- 不同机器上每个进程的两个端口（这里 `7101`/`8101`）需在**对应的网卡 IP** 上绑定；同机多进程则端口要错开（像 `config/cluster.json` 的本地示例那样 `7101/7102/...`）。
- `domainCode` 可以是任意字符串，但要在节点和客户端之间保持一致。
- 防火墙 / 安全组要放通每个节点的**两个端口**（`listenAddress` 端口 + `interDomainAddress` 端口）。

---

## 5. 在每台机器上启动节点

服务端参数（`cmd/cdraft/main.go`）：

| flag | 默认 | 说明 |
|---|---|---|
| `-config` | `config/cluster.json` | 拓扑配置路径 |
| `-node` | （必填） | 本机节点 id，必须是配置里的某个 `id` |
| `-data` | `data` | 持久化目录，状态写到 `<data>/<nodeID>.json`（目录自动创建） |

直接前台运行（每台机器各跑自己的节点）：

```bash
# 在 a1 机器上
./cdraft -config /etc/cd-raft/cluster.json -node a1 -data /var/lib/cd-raft

# 在 b1 机器上
./cdraft -config /etc/cd-raft/cluster.json -node b1 -data /var/lib/cd-raft
# ... 其余节点同理，-node 改成各自 id
```

启动日志会打印 bind 与对外宣告地址，便于核对弹性 IP 配置：`node=a1 domain=domain-a client(bind=0.0.0.0:7101 advertise=10.0.1.11:7101) inter-domain(bind=0.0.0.0:8101 advertise=100.64.0.11:8101)`。

集群**无需指定谁是 leader**：每个节点随机化超时后自动发起真实选举，先选出各域 Domain Leader，再选出全局 Global Leader。

### 用 systemd 托管（推荐生产）
`/etc/systemd/system/cd-raft@.service`：

```ini
[Unit]
Description=CD-Raft node %i
After=network-online.target

[Service]
ExecStart=/usr/local/bin/cdraft -config /etc/cd-raft/cluster.json -node %i -data /var/lib/cd-raft
Restart=always
RestartSec=1
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
# 在 a1 机器上
sudo systemctl enable --now cd-raft@a1
journalctl -u cd-raft@a1 -f   # 看日志
```

---

## 6. 验证集群起来了 & 观察选举/迁移

最直接的验证是发一条写、再读出来（见第 7 节）。要观察内部状态：

- **看日志**：每个节点的 stdout 会显示其角色变化。
- **选举落点**：哪台成为 Domain Leader / Global Leader 由真实选举决定（不是固定第一个节点），重启后可能不同。
- **迁移**：开启 `optimizer + activeMigration` 后，当负载集中到某个非 GL 域，优化器会推荐该域并通过**安全交接（catch-up handoff）**把 Global Leader 迁过去。

> 程序化健康检查：每个节点还提供 `Metrics`（`Snapshot`）和 `Status` gRPC 接口（见 `proto/cdraft.proto`），可暴露 leader、任期、提交位点、Fast Return 统计、拒绝原因、RTT、候选成本、迁移历史等。`cdraft-client` 二进制目前只做读写；要查这些指标可以写个小 gRPC 调用，或扩展客户端（需要可以让我加一个 `-status` 子命令）。

---

## 7. 启动客户端发送读写

客户端参数（`cmd/cdraft-client/main.go`）：

| flag | 默认 | 说明 |
|---|---|---|
| `-target` | `127.0.0.1:7101` | 任意节点的 `listenAddress`（非 GL 会自动重定向） |
| `-origin-domain` | `domain-b` | 客户端所在域，**应填配置里的某个 `domainCode`**（影响负载统计/优化器与就近返回） |
| `-key` | 空 | 要写或读的 key |
| `-value` | 空 | 写入值；**留空表示读** |
| `-request-id` | 空 | 稳定的请求 id；留空自动生成（重试要幂等就显式传同一个） |
| `-timeout` | `5s` | 整体请求截止时间（含跨域 RTT 与重试，跨域可适当调大） |
| `-callback-listen` | `127.0.0.1:0` | Fast Return 回调本机绑定地址 |
| `-reply-route` | 空 | Fast Return 回调对外宣告地址（Domain Leader 用它回推） |

### 写入
```bash
./cdraft-client \
  -target 10.0.1.11:7101 \
  -origin-domain domain-a \
  -key user:42 -value alice \
  -timeout 5s
# 输出：request=cli-... winner=GlobalResponse index=7 result=...
# winner 可能是 GlobalResponse 或 FastResponse（取决于是否走了 Fast Return）
```

### 读取（线性一致读，由 GL 服务）
```bash
./cdraft-client \
  -target 10.0.1.11:7101 \
  -origin-domain domain-a \
  -key user:42
# 输出：user:42=alice
```

### 跨域写 + Fast Return（要点）
当 `features.fastReturnEnabled=true` 且**客户端域与 GL 域不同**时，客户端域的 Domain Leader 会就近 Fast Return。此时回调通道必须可达：

```bash
./cdraft-client \
  -target 10.0.2.11:7101 \
  -origin-domain domain-b \
  -key k -value v \
  -callback-listen 0.0.0.0:9100 \
  -reply-route <客户端机器对 Domain Leader 可达的IP>:9100
```

- `-callback-listen`：本机绑定（用 `0.0.0.0:9100` 监听所有网卡）。
- `-reply-route`：**Domain Leader 能访问到的客户端地址**。客户端在 NAT 后面 / 跨网时必须显式设置，否则就近回推到不上。
- 如果 `fastReturnEnabled=false`，可忽略这两项，客户端只用 GL 的正常 gRPC 返回。

### 用脚本批量发压
```bash
for i in $(seq 1 100); do
  ./cdraft-client -target 10.0.1.11:7101 -origin-domain domain-a \
    -key "k$i" -value "v$i" -timeout 5s
done
```

---

## 8. 功能开关（在 `features` 里控制）

| 开关 | 作用 |
|---|---|
| `fastReturnEnabled` | 异域写就近 Fast Return（≈1 个跨域 RTT），关掉则走 GL 正常返回（≈2 个跨域 RTT） |
| `optimizerEnabled` | GL 上运行只读代价模型，给出最优域推荐（不直接动 leader） |
| `activeMigrationEnabled` | 允许优化器推荐触发安全交接迁移（必须同时开 optimizer） |

先用全 `false` 跑通基础读写，再逐个打开观察行为，更容易定位问题。

---

## 9. 故障 / 持久化 / 安全测试

- **杀节点容错**：`sudo systemctl stop cd-raft@b1`（或 `kill`）。只要每域仍有多数派、且存活的 Domain Leader ≥ `N-1`，集群应继续可读写；杀掉 GL 会触发全局重选。
- **持久化重启**：节点状态在 `<data>/<nodeID>.json`。重启进程会从该文件恢复任期/日志/已应用状态，不会丢已提交数据。测试时删掉该文件即等于“全新节点”。
- **安全**：当前 gRPC 用 **明文 insecure** 连接，没有 TLS/鉴权。**不要把端口直接暴露到公网**。务必用安全组 / 私网 / WireGuard/Tailscale 限制访问（这也是第 3 节方式 B 的额外好处）。

---

## 10. 最小可跑清单（TL;DR）

1. `go build -o bin/cdraft ./cmd/cdraft && go build -o bin/cdraft-client ./cmd/cdraft-client`
2. 写 `cluster.json`：同 VPC 用**私网 IP**；跨地域用**叠加网络虚拟 IP**，或裸**弹性 IP**（`bindInterDomainAddress=0.0.0.0:port` + `publicInterDomainAddress=弹性IP:port`，模板见 `config/cluster.cloud.example.json`），`networkSimulation.enabled=false`。
3. 分发二进制 + 配置到每台机器，放通每个节点的两个端口。
4. 每台机器 `./cdraft -config cluster.json -node <自己的id> -data /var/lib/cd-raft`。
5. `./cdraft-client -target <任一节点 listenAddress> -origin-domain <某域> -key k -value v` 写入，再不带 `-value` 读出来验证。

---

## 附录：方式 C 配置速查（bind≠advertise，已实现）

弹性 IP 部署的最小配置模板（每个节点一行，重点在最后三个可选字段）：

```json
{"id": "a1", "domainCode": "domain-a",
 "listenAddress": "10.0.1.11:7101",
 "interDomainAddress": "10.0.1.11:8101",
 "bindListenAddress": "0.0.0.0:7101",
 "bindInterDomainAddress": "0.0.0.0:8101",
 "publicInterDomainAddress": "100.64.0.11:8101"}
```

地址流向（以 a1 为例）：

| 场景 | 用到的地址 | 值 |
|---|---|---|
| a1 本机绑定（客户端口） | `bindListenAddress` | `0.0.0.0:7101` |
| a1 本机绑定（跨域口） | `bindInterDomainAddress` | `0.0.0.0:8101` |
| 同域节点拨 a1 | `listenAddress` | `10.0.1.11:7101`（私网） |
| 跨域节点拨 a1 | `publicInterDomainAddress` | `100.64.0.11:8101`（弹性 IP） |
| 客户端拨 a1 | `listenAddress` | `10.0.1.11:7101` |

要点：

- 实现位置：`internal/topology/config.go`（字段 + `ClientBindAddress()`/`InterDomainBindAddress()`/`InterDomainDialAddress()`）、`internal/cdraft/rpc_node.go`（`Start()` 用 bind 地址、`peerAddress()` 跨域用 public 地址）。
- 三个可选字段**全部留空 = 与旧版行为完全一致**，所以老配置无需改动。
- 安全组需放通每节点的两个端口；跨域口暴露在公网时，强烈建议叠加 TLS / 限制来源（当前 gRPC 默认明文，见第 9 节）。
- **弹性 IP 变更：只改对应节点的 `publicInterDomainAddress` 并重启该节点。**
