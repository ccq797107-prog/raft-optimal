# 实验：动态负载漂移下的自适应迁移收益（时间序列）

生成时间：2026-07-21 00:54:12

本文档由 `TestRealGRPCAdaptiveMigrationTimeSeries` 自动生成。负载来源域随时间漂移（`a` → `b` → `c`），在真实 gRPC、注入单向延迟的三域集群上以**完全相同的时间表跑两遍**：一遍**固定 GL（不迁移）**，一遍**自动迁移（成本模型 + Move 控制器持续追踪负载）**。下方“设计”固定，“观测结果”随重跑更新。

## 1. 假设

负载漂移到远离当前 GL 的域时，固定 GL 的客户端延迟会**阶梯式上升**；而自动迁移会把 GL 迁去追随负载，使延迟在每次漂移后**迅速回落**——证明系统能自适应跟踪负载、持续保持低延迟。

## 2. 设计

### 2.1 拓扑与延迟矩阵

本地单向延迟 1ms，跨域单向延迟（往返为其两倍）：

| 链路 | 单向延迟 | 往返 RTT |
|---|---|---|
| `a <-> b` | 50ms | 100ms |
| `a <-> c` | 80ms | 160ms |
| `b <-> c` | 60ms | 120ms |

### 2.2 负载时间表

- 初始 Global Leader：域 `a`（节点 `a1`）。
- 阶段 1：`0s`–`12s`，负载来源域 `a`（与 GL 初始域同域（基线最优））。
- 阶段 2：`12s`–`24s`，负载来源域 `b`（漂移到较近域）。
- 阶段 3：`24s`–`36s`，负载来源域 `c`（漂移到最远域（固定 GL 最不利））。

### 2.3 两遍对照

- **固定 GL**：不运行控制器、从不发 Move，GL 始终在域 `a`。
- **自动迁移**：运行与 `cmd/cdraft-mover` 相同的 discover→measure→decide→move 控制循环（成本模型 + 迟滞确认 + 冷却），由它**自主**把 GL 迁到负载域。
- 采样：每 250ms 从“当前阶段 origin”向“当前 GL”发一次写+读并记录延迟。

## 3. 如何复现

```bash
go test ./internal/cdraft/ -run TestRealGRPCAdaptiveMigrationTimeSeries -v -count=1
```

## 4. 观测结果

### 4.1 各阶段稳态延迟（取每阶段后半段中位数，避开切换瞬态）

| 阶段 | origin | 固定 GL 域 | 固定 写/读 | 自动 GL 域 | 自动 写/读 | 写收益 | 读收益 |
|---|---|---|---|---|---|---|---|
| 1 | `a` | `a` | 107ms / 2ms | `a` | 111ms / 2ms | **--4%** | **-4%** |
| 2 | `b` | `a` | 207ms / 101ms | `b` | 111ms / 2ms | **-46%** | **-98%** |
| 3 | `c` | `a` | 268ms / 161ms | `c` | 131ms / 2ms | **-51%** | **-99%** |

### 4.2 写延迟时间序列（每秒中位数，单位 ms；`F`=固定 GL，`A`=自动迁移，`#`=两者重合）

```
 592 |              A                     
 543 |              A                     
 493 |              A                     
 444 |              A                     
 395 |              A                     
 345 |              A                     
 296 |              A                     
 247 |              A         FFFFFFFFFFFF
 197 |            ###FFFFFFFFF###FFFFFFFFF
 148 |            ###FFFFFFFFF###FFFFFFFFF
  99 |####################################
  49 |####################################
     +------------------------------------
      0s          12s         24s           (秒)
```

### 4.3 读延迟时间序列（每秒中位数，单位 ms）

```
 162 |                          F F       
 148 |                        FFFFFFFFFFFF
 135 |                        FFFFFFFFFFFF
 122 |                        FFFFFFFFFFFF
 108 |                        ###FFFFFFFFF
  94 |            ###FFFFFFFFF###FFFFFFFFF
  81 |            ###FFFFFFFFF###FFFFFFFFF
  68 |            ###FFFFFFFFF###FFFFFFFFF
  54 |            ###FFFFFFFFF###FFFFFFFFF
  40 |            ###FFFFFFFFF###FFFFFFFFF
  27 |            ###FFFFFFFFF###FFFFFFFFF
  14 |            ###FFFFFFFFF###FFFFFFFFF
     +------------------------------------
      0s          12s         24s           (秒)
```

### 4.4 自动迁移的 GL 位置随时间变化

```
GL域 |aaaaaaaaaaaaaaabbbbbbbbbbbbccccccccc
负载 |aaaaaaaaaaaabbbbbbbbbbbbcccccccccccc
     ------------------------------------
```

## 5. 结论

- 负载漂移时，固定 GL 的客户端延迟随“负载域离 GL 越来越远”而阶梯上升；自动迁移在每次漂移后把 GL 迁到负载域，延迟迅速回落并维持低位。
- 控制器仅凭节点发布的 Metrics **自主**决策，迁移走安全交接路径；切换瞬间有短暂延迟尖峰（追平 + 选举窗口），随后稳定。
- 原始逐秒数据见同目录 `adaptive-migration-timeseries.csv`，可用任意工具绘图。
