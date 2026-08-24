package cdraft

import (
	"context"
	"fmt"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
)

// metricsService exposes the observability surface required to verify the core
// mechanisms AND to feed the external migration controller: leaders and terms,
// quorum/commit/apply positions, Fast Return and degradation counts, rejection
// reasons, the per-domain request distribution (W/R window stats) and the
// measured inter-domain RTT matrix, plus migration history. The cost model is
// intentionally NOT computed here; cmd/cdraft-mover derives candidate costs from
// these raw signals. This node-side endpoint reports what the node itself sees.
type metricsService struct {
	cdraftv1.UnimplementedMetricsServer
	node *RPCNode
}

func (s *metricsService) Snapshot(_ context.Context, _ *cdraftv1.NodeStatusRequest) (*cdraftv1.MetricsResponse, error) {
	return s.node.metricsSnapshot(), nil
}

func (n *RPCNode) metricsSnapshot() *cdraftv1.MetricsResponse {
	now := time.Now()
	matrix := n.rtt.snapshot()
	writes, reads := n.stats.counts(now)
	floatingMatrix := n.floatingLatency.freshSnapshot(now)
	floatingLatencyAge := n.floatingLatency.ageMillis(now)

	n.mu.Lock()
	defer n.mu.Unlock()

	domainLeaders := make(map[string]string, len(n.domainLeaders))
	for domain, identity := range n.domainLeaders {
		domainLeaders[domain] = identity.NodeID
	}
	quorum := make(map[string]uint64, len(n.state.DomainQuorumIndex))
	for domain, index := range n.state.DomainQuorumIndex {
		quorum[domain] = index
	}
	rejections := make(map[string]uint64, len(n.metrics.rejections))
	for reason, count := range n.metrics.rejections {
		rejections[reason] = count
	}
	floatingResponderByDomain := make(map[string]uint64, len(n.metrics.floatingResponderByDomain))
	for domain, count := range n.metrics.floatingResponderByDomain {
		floatingResponderByDomain[domain] = count
	}
	writesByDomain := make(map[string]uint64, len(writes))
	for domain, count := range writes {
		writesByDomain[domain] = uint64(count)
	}
	readsByDomain := make(map[string]uint64, len(reads))
	for domain, count := range reads {
		readsByDomain[domain] = uint64(count)
	}
	rttMillis := make(map[string]uint64)
	for from, row := range matrix {
		for to, oneWay := range row {
			rttMillis[fmt.Sprintf("%s->%s", from, to)] = uint64(2 * oneWay)
		}
	}
	for from, row := range floatingMatrix {
		for to, oneWay := range row {
			rttMillis[fmt.Sprintf("%s->%s", from, to)] = uint64(2 * oneWay)
		}
	}

	return &cdraftv1.MetricsResponse{
		NodeId:                    n.local.ID,
		DomainCode:                n.local.DomainCode,
		Stage:                     string(n.stage),
		GlobalLeader:              toPBDomainIdentity(n.globalLeader.DomainLeader),
		GlobalTerm:                n.state.GlobalTerm,
		DomainLeaders:             domainLeaders,
		DomainQuorumIndex:         quorum,
		KnownGlobalCommitIndex:    n.state.KnownGlobalCommitIndex,
		AppliedIndex:              n.state.AppliedIndex,
		FastReturnSuccess:         n.metrics.fastReturnSuccess,
		FastReturnDegraded:        n.metrics.fastReturnDegraded,
		RejectionsByReason:        rejections,
		WriteRequestsByDomain:     writesByDomain,
		ReadRequestsByDomain:      readsByDomain,
		InterDomainRttMillis:      rttMillis,
		Migrations:                n.migrationsCount,
		LastMigration:             n.lastMigrationNote,
		SnapshotLastIndex:         n.state.SnapshotLastIndex,
		FloatingLatencyAgeMillis:  floatingLatencyAge,
		FloatingResponderSelected: n.metrics.floatingResponderChosen,
		FloatingResponderDegraded: n.metrics.floatingResponderFailed,
		FloatingRaceGlobalWins:    n.metrics.floatingRaceGlobalWins,
		FloatingRaceFastWins:      n.metrics.floatingRaceFastWins,
		FloatingResponderByDomain: floatingResponderByDomain,
	}
}
