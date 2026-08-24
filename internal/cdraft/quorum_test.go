package cdraft

import "testing"

// TestGlobalElectionThresholdIsQuorum guards the split-brain fix: the global
// election threshold must be a true quorum for every supported domain count, so
// two partitions can never each elect a Global Leader in the same term. The bug
// it regresses: N=2 used N-1=1, letting each domain self-elect (a1 term=1 AND
// b1 term=1 simultaneously).
func TestGlobalElectionThresholdIsQuorum(t *testing.T) {
	for n := 2; n <= 7; n++ {
		th := globalElectionThreshold(n)
		// Quorum intersection: any two threshold-sized subsets of N must overlap,
		// i.e. 2*threshold > N. Otherwise two disjoint partitions could each
		// gather a "quorum" and both elect a leader.
		if 2*th <= n {
			t.Fatalf("N=%d threshold=%d is not a quorum (2*th must exceed N)", n, th)
		}
		// Preserve the paper's one-domain-fault tolerance (N-1) where it is still
		// a quorum, i.e. for N>=3.
		if n >= 3 && th != n-1 {
			t.Fatalf("N=%d threshold=%d, want N-1=%d", n, th, n-1)
		}
		// N=2 cannot tolerate a domain fault and stay safe: it must require both.
		if n == 2 && th != 2 {
			t.Fatalf("N=2 threshold=%d, want 2 (both domains must agree)", th)
		}
	}
}
