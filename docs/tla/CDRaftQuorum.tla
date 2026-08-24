----------------------------- MODULE CDRaftQuorum -----------------------------
(***************************************************************************)
(* CD-Raft inter-domain quorum boundary model.                              *)
(*                                                                         *)
(* This spec separates two quorum obligations that are easy to conflate:    *)
(*                                                                         *)
(*   1. Fast Return preservation: a future election quorum must intersect   *)
(*      the two domains that durably held a fast-returned entry.            *)
(*   2. Leader uniqueness: any two election quorums in the same global term *)
(*      must intersect each other.                                          *)
(*                                                                         *)
(* N-1 satisfies (1) for N >= 2, but it does not satisfy (2) when N = 2.     *)
(* The safe election quorum is therefore max(N-1, 2). For N > 2 this is     *)
(* just N-1; only the two-domain case is strengthened to 2-of-2.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Domains

ASSUME Cardinality(Domains) >= 2

VARIABLES
    q1,
    q2,
    holders

vars == << q1, q2, holders >>

Init ==
    /\ q1 \in SUBSET Domains
    /\ q2 \in SUBSET Domains
    /\ holders \in SUBSET Domains

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

IsNMinusOneQuorum(Q) ==
    /\ Q \subseteq Domains
    /\ Cardinality(Q) >= Cardinality(Domains) - 1

IsAtLeastTwoQuorum(Q) ==
    /\ Q \subseteq Domains
    /\ Cardinality(Q) >= 2

\* Equivalent to |Q| >= max(N-1, 2).
IsSafeElectionQuorum(Q) ==
    /\ IsNMinusOneQuorum(Q)
    /\ IsAtLeastTwoQuorum(Q)

IsFastReturnHolderSet(H) ==
    /\ H \subseteq Domains
    /\ Cardinality(H) = 2

TypeOK ==
    /\ q1 \subseteq Domains
    /\ q2 \subseteq Domains
    /\ holders \subseteq Domains

\* Safe election quorums always intersect, including N=2.
SafeElectionQuorumsIntersect ==
    (IsSafeElectionQuorum(q1) /\ IsSafeElectionQuorum(q2)) =>
        q1 \cap q2 # {}

\* Safe election quorums always hit a two-domain Fast Return holder set.
SafeElectionHitsFastReturnHolders ==
    (IsFastReturnHolderSet(holders) /\ IsSafeElectionQuorum(q1)) =>
        holders \cap q1 # {}

\* This is true for Fast Return preservation, even at N=2.
NMinusOneHitsFastReturnHolders ==
    (IsFastReturnHolderSet(holders) /\ IsNMinusOneQuorum(q1)) =>
        holders \cap q1 # {}

\* This intentionally fails at N=2: {d1} and {d2} are both N-1 quorums.
NMinusOneElectionQuorumsIntersect ==
    (IsNMinusOneQuorum(q1) /\ IsNMinusOneQuorum(q2)) =>
        q1 \cap q2 # {}

==============================================================================
