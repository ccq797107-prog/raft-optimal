-------------------------------- MODULE CDRaft --------------------------------
(***************************************************************************)
(* CD-Raft Fast Return and dual-index read model.                           *)
(*                                                                         *)
(* The model checks Fast Return safety and read-barrier safety.             *)
(* In-domain replication is represented by DomainCatchUp. Elections are     *)
(* represented by BumpTerm with a candidate restriction. RPC transport,     *)
(* persistence, and timeout behavior are outside the model.                 *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Domains,         \* finite set of domains
    GlobalDomain,    \* domain hosting the Global Leader
    RequestIDs,      \* finite set of request IDs
    Keys,            \* finite set of keys
    Values,          \* finite set of values
    NoVal,           \* unwritten value
    NoReq,           \* empty pendingFR slot
    MaxLogLen,       \* glLog bound
    MaxTerm          \* global term bound

ASSUME GlobalDomain \in Domains
ASSUME Cardinality(Domains) >= 2
ASSUME NoVal \notin Values
ASSUME NoReq \notin RequestIDs
ASSUME MaxLogLen \in Nat \ {0}
ASSUME MaxTerm   \in Nat \ {0}

\* Log entry
Entry(t,i,r,k,v) == [term |-> t, idx |-> i, reqID |-> r, key |-> k, val |-> v]
NilEntry         == [term |-> 0, idx |-> 0, reqID |-> NoReq,
                    key |-> CHOOSE k \in Keys : TRUE, val |-> NoVal]

\* Entry identity used by Fast Return validation.
EntryEq(a,b) == /\ a.term  = b.term
                /\ a.idx   = b.idx
                /\ a.reqID = b.reqID

VARIABLES
    glLog,         \* Global Leader log
    glTerm,        \* global term
    commitIndex,   \* known global commit index
    appliedIndex,  \* applied index
    stateMachine,  \* [Keys -> Values \cup {NoVal}]
    resultsMap,    \* [RequestIDs -> Entry \cup {NilEntry}]
    dlog,          \* quorum-held prefix per domain
    pendingFR,     \* [Domains -> [1..MaxLogLen -> Entry \cup {NilEntry}]]
    fastSent,      \* request IDs delivered by Fast Return
    clientResult   \* [RequestIDs -> Entry \cup {NilEntry}]

vars == << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
           resultsMap, dlog, pendingFR, fastSent, clientResult >>

DQI(d) == Len(dlog[d])                    \* DomainQuorumIndex of d

\* Domain quorum holds entry e at index i.
DomainHoldsAt(d, i, e) ==
    /\ i \in 1..DQI(d)
    /\ EntryEq(dlog[d][i], e)

\* Domain quorum holds the current Global Leader prefix 1..upto.
DomainHoldsPrefix(d, upto) ==
    /\ upto \in 0..Len(glLog)
    /\ upto <= DQI(d)
    /\ \A j \in 1..upto: DomainHoldsAt(d, j, glLog[j])

\* Two-domain quorum evidence.
TwoDomainsHold(i, e) ==
    \E d1, d2 \in Domains:
        /\ d1 # d2
        /\ DomainHoldsAt(d1, i, e)
        /\ DomainHoldsAt(d2, i, e)

\* Global Domain plus one remote domain hold the same current prefix.
TwoDomainPrefixEvidence(upto) ==
    /\ DomainHoldsPrefix(GlobalDomain, upto)
    /\ \E d \in Domains \ {GlobalDomain}: DomainHoldsPrefix(d, upto)

\* Fast Return evidence for a specific responder and entry.
FastReturnPrefixEvidence(d, i, e) ==
    /\ d \in Domains \ {GlobalDomain}
    /\ i \in 1..Len(glLog)
    /\ EntryEq(glLog[i], e)
    /\ DomainHoldsPrefix(GlobalDomain, i)
    /\ DomainHoldsPrefix(d, i)

\* Fill results for every entry in the committed prefix.
CommittedResultsTo(upto) ==
    [r \in RequestIDs |->
        IF \E j \in 1..upto: glLog[j].reqID = r
        THEN LET J == CHOOSE j \in 1..upto: glLog[j].reqID = r
             IN glLog[J]
        ELSE resultsMap[r]]

\* Latest value for k in glLog[1..upto].
LatestCommittedValue(k, upto) ==
    IF \E i \in 1..upto: glLog[i].key = k
    THEN LET LastI == CHOOSE i \in 1..upto :
                         /\ glLog[i].key = k
                         /\ \A j \in (i+1)..upto: glLog[j].key # k
         IN glLog[LastI].val
    ELSE NoVal

Init ==
    /\ glLog        = << >>
    /\ glTerm       = 1
    /\ commitIndex  = 0
    /\ appliedIndex = 0
    /\ stateMachine = [k \in Keys      |-> NoVal]
    /\ resultsMap   = [r \in RequestIDs |-> NilEntry]
    /\ dlog         = [d \in Domains   |-> << >>]
    /\ pendingFR    = [d \in Domains   |-> [i \in 1..MaxLogLen |-> NilEntry]]
    /\ fastSent     = {}
    /\ clientResult = [r \in RequestIDs |-> NilEntry]

\* Append a client write. reqID is unique in the current log.
AppendEntry(reqID, k, v) ==
    /\ Len(glLog) < MaxLogLen
    /\ \A i \in 1..Len(glLog): glLog[i].reqID # reqID
    /\ LET e == Entry(glTerm, Len(glLog)+1, reqID, k, v) IN
       glLog' = Append(glLog, e)
    /\ UNCHANGED << glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, dlog, pendingFR, fastSent, clientResult >>

\* Adopt a GL prefix without decreasing the domain quorum index.
DomainCatchUp(d, i) ==
    /\ i \in 0..Len(glLog)
    /\ i >= DQI(d)
    /\ dlog[d] # SubSeq(glLog, 1, i)
    /\ dlog' = [dlog EXCEPT ![d] = SubSeq(glLog, 1, i)]
    /\ UNCHANGED << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, pendingFR, fastSent, clientResult >>

\* Publish the Global Domain quorum certificate.
SendFRCertificate(d, i) ==
    /\ d \in Domains \ {GlobalDomain}
    /\ i \in 1..Len(glLog)
    /\ DomainHoldsAt(GlobalDomain, i, glLog[i])
    /\ pendingFR' = [pendingFR EXCEPT ![d][i] = glLog[i]]
    /\ UNCHANGED << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, dlog, fastSent, clientResult >>

\* Deliver Fast Return after term, continuity, and entry-identity checks.
DeliverFR(d, i) ==
    LET p == pendingFR[d][i] IN
    /\ i \in 1..MaxLogLen
    /\ p.reqID # NoReq
    /\ p.term  >= glTerm
    /\ FastReturnPrefixEvidence(d, i, p)
    /\ p.reqID \notin fastSent
    /\ commitIndex'  = IF commitIndex < i THEN i ELSE commitIndex
    /\ resultsMap'   = IF commitIndex < i THEN CommittedResultsTo(i)
                       ELSE resultsMap
    /\ fastSent'     = fastSent \cup {p.reqID}
    /\ clientResult' = [clientResult EXCEPT ![p.reqID] = p]
    /\ UNCHANGED << glLog, glTerm, appliedIndex, stateMachine, dlog,
                    pendingFR >>

\* Commit the next entry with Global Domain and one remote-domain quorum.
AdvanceCommit ==
    /\ commitIndex < Len(glLog)
    /\ LET i == commitIndex + 1 IN
        /\ TwoDomainPrefixEvidence(i)
        /\ commitIndex' = i
        /\ resultsMap'  = [resultsMap EXCEPT ![glLog[i].reqID] = glLog[i]]
    /\ UNCHANGED << glLog, glTerm, appliedIndex, stateMachine,
                    dlog, pendingFR, fastSent, clientResult >>

\* Commit a proven prefix in one step.
AdvanceCommitTo(target) ==
    /\ target \in (commitIndex+1)..Len(glLog)
    /\ TwoDomainPrefixEvidence(target)
    /\ commitIndex' = target
    /\ resultsMap'  = CommittedResultsTo(target)
    /\ UNCHANGED << glLog, glTerm, appliedIndex, stateMachine,
                    dlog, pendingFR, fastSent, clientResult >>

\* Apply one committed entry in order.
ApplyOne ==
    /\ appliedIndex < commitIndex
    /\ LET i == appliedIndex + 1
           e == glLog[i] IN
        /\ stateMachine' = [stateMachine EXCEPT ![e.key] = e.val]
        /\ appliedIndex' = i
    /\ UNCHANGED << glLog, glTerm, commitIndex, resultsMap, dlog, pendingFR,
                    fastSent, clientResult >>

\* Change term and truncate only entries without two-domain quorum evidence.
BumpTerm ==
    /\ glTerm + 1 <= MaxTerm
    /\ \E L \in commitIndex..Len(glLog):
         /\ \A i \in (L+1)..Len(glLog):
               \A d1, d2 \in Domains:
                   (d1 # d2 /\ DQI(d1) >= i /\ DQI(d2) >= i)
                       => \neg EntryEq(dlog[d1][i], dlog[d2][i])
         /\ glLog'   = SubSeq(glLog, 1, L)
         /\ glTerm'  = glTerm + 1
    /\ UNCHANGED << commitIndex, appliedIndex, stateMachine, resultsMap, dlog,
                    pendingFR, fastSent, clientResult >>

Next ==
    \/ \E r \in RequestIDs, k \in Keys, v \in Values: AppendEntry(r, k, v)
    \/ \E d \in Domains,    i \in 0..MaxLogLen:       DomainCatchUp(d, i)
    \/ \E d \in Domains,    i \in 1..MaxLogLen:       SendFRCertificate(d, i)
    \/ \E d \in Domains,    i \in 1..MaxLogLen:       DeliverFR(d, i)
    \/ AdvanceCommit
    \/ \E i \in 1..MaxLogLen:                          AdvanceCommitTo(i)
    \/ ApplyOne
    \/ BumpTerm

Spec == Init /\ [][Next]_vars

\* Weak fairness for commit and apply progress.
Fairness == WF_vars(AdvanceCommit) /\ WF_vars(ApplyOne)

LiveSpec == Spec /\ Fairness

\* Type invariant.
TypeInv ==
    /\ Len(glLog) \in 0..MaxLogLen
    /\ \A i \in 1..Len(glLog):
         /\ glLog[i].term  \in 1..MaxTerm
         /\ glLog[i].idx   = i
         /\ glLog[i].reqID \in RequestIDs
         /\ glLog[i].key   \in Keys
         /\ glLog[i].val   \in Values
    /\ glTerm       \in 1..MaxTerm
    /\ commitIndex  \in 0..Len(glLog)
    /\ appliedIndex \in 0..commitIndex
    /\ fastSent \subseteq RequestIDs
    /\ \A d \in Domains: Len(dlog[d]) \in 0..MaxLogLen

CommitMonotone == appliedIndex <= commitIndex /\ commitIndex <= Len(glLog)

PendingFRWellFormed ==
    \A d \in Domains, i \in 1..MaxLogLen:
        pendingFR[d][i].reqID # NoReq => pendingFR[d][i].idx = i

\* A Fast Return result remains at the same GL log index.
FastReturnSafety ==
    \A r \in fastSent:
        LET c == clientResult[r] IN
        /\ c.reqID = r
        /\ c.idx \in 1..Len(glLog)
        /\ EntryEq(glLog[c.idx], c)

\* Committed Fast Return results agree with resultsMap.
FastReturnAgreesWithCommit ==
    \A r \in fastSent:
        LET c == clientResult[r] IN
        (c.idx <= commitIndex) =>
            /\ EntryEq(glLog[c.idx], c)
            /\ EntryEq(resultsMap[r], c)

\* Reads passing the dual-index barrier observe the applied log prefix.
DualIndexReadSafety ==
    LET barrier == DQI(GlobalDomain) IN
    (commitIndex >= barrier /\ appliedIndex >= barrier) =>
        \A k \in Keys:
            stateMachine[k] = LatestCommittedValue(k, appliedIndex)

\* State machine matches the applied prefix.
StateMachineCoherent ==
    \A k \in Keys:
        stateMachine[k] = LatestCommittedValue(k, appliedIndex)

\* appliedIndex <= commitIndex <= Global Domain quorum index.
IndexOrderingInvariant ==
    /\ appliedIndex <= commitIndex
    /\ commitIndex  <= DQI(GlobalDomain)

\* Fast Return results eventually commit under Fairness.
FastReturnEventuallyCommits ==
    \A r \in RequestIDs:
        (r \in fastSent) ~> (clientResult[r].idx <= commitIndex)

\* Fast Return results eventually apply under Fairness.
FastReturnEventuallyApplied ==
    \A r \in RequestIDs:
        (r \in fastSent) ~> (clientResult[r].idx <= appliedIndex)

================================================================================
