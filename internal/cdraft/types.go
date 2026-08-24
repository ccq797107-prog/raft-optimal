package cdraft

import (
	"errors"
	"fmt"
	"time"
)

// Stage 描述节点从启动到可对外服务所经历的协议阶段。
//
// Stage 与 Role 的含义不同：Stage 表示整个节点当前走到了哪一步，Role
// 则表示节点在某一级选举中扮演 Follower、Candidate 还是 Leader。
type Stage string

const (
	// Booting 表示节点仍在加载持久化状态、初始化网络等启动流程中。
	Booting Stage = "Booting"
	// DomainElecting 表示节点正在参与本域内的 Domain Leader 选举。
	DomainElecting Stage = "DomainElecting"
	// DomainReady 表示本域已经产生可用的 Domain Leader。
	DomainReady Stage = "DomainReady"
	// GlobalElecting 表示各域的 Domain Leader 正在选举 Global Leader。
	GlobalElecting Stage = "GlobalElecting"
	// Serving 表示节点已经通过服务门禁，可以参与正常读写流程。
	Serving Stage = "Serving"
)

// Role 是节点在一次 Raft 风格选举中的角色。
// 同一组角色值会分别用于域内选举和全局选举，具体属于哪一级由保存它的
// 状态字段或调用上下文决定。
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// MessageKind 标识协议消息的逻辑类别，主要用于网络策略、故障注入、
// 链路延迟模拟和可观测性统计，而不是消息的实际载荷类型。
type MessageKind string

const (
	DomainVoteMessage            MessageKind = "DomainVote"
	DomainHeartbeatMessage       MessageKind = "DomainHeartbeat"
	DomainReadyMessage           MessageKind = "DomainReady"
	GlobalVoteMessage            MessageKind = "GlobalVote"
	GlobalHeartbeatMessage       MessageKind = "GlobalHeartbeat"
	ReplicateEntryMessage        MessageKind = "ReplicateEntry"
	DomainQuorumAckMessage       MessageKind = "DomainQuorumAck"
	GlobalDomainQuorumAckMessage MessageKind = "GlobalDomainQuorumAck"
	CommitNoticeMessage          MessageKind = "CommitNotice"
)

// RPCPolicy 定义一次节点间 RPC 的超时和重试策略。
type RPCPolicy struct {
	Deadline   time.Duration // 单次尝试允许的最长时间。
	MaxRetries int           // 首次失败后最多重试的次数。
}

// DefaultRPCPolicy 返回节点间 RPC 的默认策略。
func DefaultRPCPolicy() RPCPolicy {
	return RPCPolicy{Deadline: 500 * time.Millisecond, MaxRetries: 3}
}

var (
	// 以下错误是协议层的稳定错误分类，调用方可用 errors.Is 判断失败原因。
	ErrNodeStopped        = errors.New("node is stopped")
	ErrNoDomainQuorum     = errors.New("domain majority is unavailable")
	ErrNotDomainLeader    = errors.New("node is not the current domain leader")
	ErrNotGlobalLeader    = errors.New("node is not the current global leader")
	ErrNoGlobalQuorum     = errors.New("N-1 domain leaders are unavailable")
	ErrNotServing         = errors.New("node has not passed the serving gate")
	ErrNoCommit           = errors.New("two-domain commit evidence is unavailable")
	ErrReadBarrier        = errors.New("linearizable read barrier is not satisfied")
	ErrDeadline           = errors.New("response race deadline exceeded")
	ErrBothPathsFailed    = errors.New("both response paths failed")
	ErrInvalidResult      = errors.New("invalid client result")
	ErrInconsistentResult = errors.New("successful response paths disagree")
)

// LogSummary 是节点日志末尾的摘要，用于选举时比较候选人的日志新旧。
// 它使用全局日志坐标，因为所有共识域复制的是同一条全局日志，并不存在
// 每个域各自独立编号的业务日志。
type LogSummary struct {
	LastGlobalTerm  uint64 // 最后一条全局日志由哪个 Global Leader 任期产生。
	LastGlobalIndex uint64 // 最后一条全局日志在统一日志中的位置。
}

// AtLeast 按 Raft 的“先比较末条任期，再比较索引”规则判断 l 是否至少
// 与 other 一样新。该方法用于投票门禁，避免日志较旧的候选人当选。
func (l LogSummary) AtLeast(other LogSummary) bool {
	if l.LastGlobalTerm != other.LastGlobalTerm {
		return l.LastGlobalTerm > other.LastGlobalTerm
	}
	return l.LastGlobalIndex >= other.LastGlobalIndex
}

// DomainLeaderIdentity 唯一描述某个域在某个域内任期中的 Leader。
// DomainTerm 是领导权的 fencing token：即使 DomainID 和 NodeID 相同，
// 不同 DomainTerm 也代表不同的一届领导权。
type DomainLeaderIdentity struct {
	DomainID   string // 所属共识域。
	NodeID     string // 担任 Domain Leader 的节点。
	DomainTerm uint64 // 该身份生效时的域内选举任期，不是日志条目的任期。
}

// Valid 判断身份是否包含可用于协议校验的全部字段。
func (id DomainLeaderIdentity) Valid() bool {
	return id.DomainID != "" && id.NodeID != "" && id.DomainTerm > 0
}

// GlobalLeaderIdentity 描述当前 Global Leader。
// Global Leader 必然先是某个域的合法 Domain Leader，所以这里嵌入完整的
// DomainLeaderIdentity，再用 GlobalTerm 区分不同届的全局领导权。
type GlobalLeaderIdentity struct {
	DomainLeader DomainLeaderIdentity // Global Leader 的域内领导身份。
	GlobalTerm   uint64               // 全局选举任期，也是新日志条目的任期。
}

// Valid 判断全局身份和其内嵌的域内身份是否都有效。
func (id GlobalLeaderIdentity) Valid() bool {
	return id.DomainLeader.Valid() && id.GlobalTerm > 0
}

// DomainVoteRequest 是域内选举请求。
type DomainVoteRequest struct {
	DomainID   string     // 选举发生在哪个域。
	Candidate  string     // 候选节点 ID。
	DomainTerm uint64     // 候选人发起竞选的域内任期。
	Log        LogSummary // 候选人的全局日志末尾，用于日志新旧校验。
}

// DomainVoteResponse 是域内投票结果；返回任期可让落后的请求方及时更新状态。
type DomainVoteResponse struct {
	DomainTerm uint64
	Granted    bool
}

// GlobalVoteRequest 是 Domain Leaders 之间的全局选举请求。
type GlobalVoteRequest struct {
	Candidate  DomainLeaderIdentity // 参选者必须是当前有效的 Domain Leader。
	GlobalTerm uint64               // 候选人发起竞选的全局任期。
	Log        LogSummary           // 候选人的日志摘要。
}

// GlobalVoteResponse 是某个 Domain Leader 对全局选举的投票结果。
type GlobalVoteResponse struct {
	Voter      DomainLeaderIdentity
	GlobalTerm uint64
	Granted    bool
}

// Command 是提交给复制状态机的最小 KV 写命令。
type Command struct {
	Key   string // 要写入的键。
	Value string // 要写入的值。
}

// LogEntry 是系统唯一全局日志中的一条记录。
//
// 它刻意不携带 DomainTerm 或 DomainIndex：日志是跨域共享的全局资产，
// 各域复制同一个 (GlobalTerm, GlobalIndex) 条目。某个域是否已经由多数派
// 持有该条目，不写回 LogEntry，而由持久状态中的
// DomainQuorumIndex[domain] 高水位表示。该高水位结合本地 Log 中条目的
// GlobalTerm 和 RequestID，才构成“本域多数派持有具体条目”的完整证据。
type LogEntry struct {
	GlobalTerm   uint64  // 创建该条目时 Global Leader 的任期。
	GlobalIndex  uint64  // 条目在唯一全局日志中的连续位置，从 1 开始。
	RequestID    string  // 客户端幂等键，也参与同一索引处的条目身份校验。
	OriginDomain string  // 请求最初来自哪个域；用于路由、统计和 Fast Return。
	Command      Command // 提交后应用到状态机的确定性命令。
	Result       string  // 命令的确定性结果，随日志复制以保证响应一致。
}

// ClientWriteRequest 是客户端发给 Global Leader 的写请求。
type ClientWriteRequest struct {
	RequestID    string  // 请求幂等键；重试必须复用同一个值。
	OriginDomain string  // 客户端所在域。
	ReplyRoute   string  // 可选的 Fast Return 回调地址；为空则只走普通响应。
	Command      Command // 待复制和提交的写命令。
}

// ResultSource 标识客户端结果经由哪条响应路径到达。
type ResultSource string

const (
	// GlobalResponse 是 Global Leader 在完成跨域提交后返回的普通响应。
	GlobalResponse ResultSource = "global-leader"
	// FastResponse 是满足两域证据后，由指定 Domain Leader 返回的快速响应。
	FastResponse ResultSource = "domain-leader-fast-return"
)

// ClientResult 是一条已经形成提交决定的客户端可见结果。
type ClientResult struct {
	RequestID       string       // 对应的客户端请求。
	GlobalTerm      uint64       // 决定所在日志条目的全局任期。
	GlobalIndex     uint64       // 决定所在日志条目的全局索引。
	Result          string       // 状态机命令的确定性结果。
	Source          ResultSource // 普通返回或 Fast Return。
	Committed       bool         // 只有已满足提交条件的结果才允许交给客户端。
	ResponderDomain string       // Fast Return 的响应域；普通响应时可以为空。
}

// Validate 校验结果是否可以作为 requestID 对应请求的成功决定。
// 它只验证结构和来源，不重新执行 quorum/commit 协议。
func (r ClientResult) Validate(requestID string) error {
	if !r.Committed || r.RequestID != requestID || r.GlobalTerm == 0 || r.GlobalIndex == 0 {
		return ErrInvalidResult
	}
	if r.Source != GlobalResponse && r.Source != FastResponse {
		return ErrInvalidResult
	}
	return nil
}

// SameDecision 判断两条不同响应路径返回的是否为同一个共识决定。
// Source 和 ResponderDomain 不参与比较，因为它们只描述传输路径；真正的
// 决定身份由 RequestID、日志任期、日志索引和确定性结果共同确定。
func (r ClientResult) SameDecision(other ClientResult) bool {
	return r.RequestID == other.RequestID &&
		r.GlobalTerm == other.GlobalTerm &&
		r.GlobalIndex == other.GlobalIndex &&
		r.Result == other.Result
}

// WriteOutcome 汇总普通响应和 Fast Return 两条可能并发完成的路径。
// 指针为空或对应 Err 非空表示该路径没有产生可用结果。
type WriteOutcome struct {
	Normal    *ClientResult
	NormalErr error
	Fast      *ClientResult
	FastErr   error
}

// majority 返回 size 个成员的严格多数派数量。
func majority(size int) int {
	return size/2 + 1
}

// globalElectionThreshold is the number of distinct Domain Leaders that must
// back a Global Leader (to campaign, to win, and to confirm an incumbent still
// holds a quorum). It MUST be a true quorum so two partitions can never each
// elect a Global Leader in the same global term.
//
// The paper specifies N-1 (tolerate one whole domain failing). For N>=3 that is
// already a strict quorum (N-1 > N/2). For N=2, however, N-1 == 1 is NOT a
// quorum: each of the two domains could self-elect on its own vote, producing
// two same-term Global Leaders (split brain). The general safe rule is therefore
// max(N-1, majority(N)) — which keeps the paper's N-1 for N>=3 and requires both
// domains (2 votes) when N==2.
func globalElectionThreshold(domains int) int {
	if domains <= 1 {
		return domains
	}
	if q := majority(domains); domains-1 < q {
		return q
	}
	return domains - 1
}

// commandResult 生成随日志复制的确定性结果表示。
// 所有副本使用相同函数，保证普通响应与 Fast Return 可以比较同一决定。
func commandResult(command Command) string {
	return fmt.Sprintf("%s=%s", command.Key, command.Value)
}
