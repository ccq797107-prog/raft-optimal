-------------------------------- MODULE CDRaftForking --------------------------------
(***************************************************************************)
(* CD-Raft model with domain-log fork recovery.                            *)
(*                                                                         *)
(* Domains may replace divergent tails above commitIndex. everTwoHeld      *)
(* records entries that obtained two-domain quorum evidence. BumpTerm      *)
(* preserves those entries.                                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Domains, GlobalDomain, RequestIDs, Keys, Values,
    NoVal, NoReq, MaxLogLen, MaxTerm

ASSUME GlobalDomain \in Domains
ASSUME Cardinality(Domains) >= 2
ASSUME NoVal \notin Values
ASSUME NoReq \notin RequestIDs
ASSUME MaxLogLen \in Nat \ {0}
ASSUME MaxTerm   \in Nat \ {0}

Entry(t,i,r,k,v) == [term |-> t, idx |-> i, reqID |-> r, key |-> k, val |-> v]
NilEntry         == [term |-> 0, idx |-> 0, reqID |-> NoReq,
                    key |-> CHOOSE k \in Keys : TRUE, val |-> NoVal]

EntryEq(a,b) == /\ a.term  = b.term
                /\ a.idx   = b.idx
                /\ a.reqID = b.reqID

VARIABLES
    glLog, glTerm, commitIndex, appliedIndex, stateMachine,
    resultsMap, dlog, pendingFR, fastSent, clientResult,
    everTwoHeld   \* entries previously held by two domains

vars == << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
           resultsMap, dlog, pendingFR, fastSent, clientResult,
           everTwoHeld >>

DQI(d) == Len(dlog[d])

DomainHoldsAt(d, i, e) ==
    /\ i \in 1..DQI(d)
    /\ EntryEq(dlog[d][i], e)

\* Entries held by two distinct domains.
TwoDistinctHeldOf(dv) ==
    LET EntriesIn(d) == { dv[d][i] : i \in 1..Len(dv[d]) }
    IN  UNION { IF d1 = d2 THEN {} ELSE EntriesIn(d1) \cap EntriesIn(d2)
                  : d1 \in Domains, d2 \in Domains }

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
    /\ everTwoHeld  = {}

AppendEntry(reqID, k, v) ==
    /\ Len(glLog) < MaxLogLen
    /\ \A i \in 1..Len(glLog): glLog[i].reqID # reqID
    /\ LET e == Entry(glTerm, Len(glLog)+1, reqID, k, v) IN
       glLog' = Append(glLog, e)
    /\ UNCHANGED << glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, dlog, pendingFR, fastSent, clientResult,
                    everTwoHeld >>

\* A domain may shrink only when its tail conflicts with glLog.
ShrinkJustified(d) ==
    \E j \in 1..Len(dlog[d]):
        IF j > Len(glLog) THEN TRUE
                          ELSE \neg EntryEq(dlog[d][j], glLog[j])

DomainCatchUp(d, i) ==
    /\ i \in commitIndex..Len(glLog)
    /\ (i >= DQI(d) \/ ShrinkJustified(d))
    /\ dlog[d] # SubSeq(glLog, 1, i)
    /\ dlog' = [dlog EXCEPT ![d] = SubSeq(glLog, 1, i)]
    /\ everTwoHeld' = everTwoHeld \cup TwoDistinctHeldOf(dlog')
    /\ UNCHANGED << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, pendingFR, fastSent, clientResult >>

SendFRCertificate(d, i) ==
    /\ d \in Domains \ {GlobalDomain}
    /\ i \in 1..Len(glLog)
    /\ DomainHoldsAt(GlobalDomain, i, glLog[i])
    /\ pendingFR' = [pendingFR EXCEPT ![d][i] = glLog[i]]
    /\ UNCHANGED << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, dlog, fastSent, clientResult, everTwoHeld >>

DeliverFR(d, i) ==
    LET p == pendingFR[d][i] IN
    /\ i \in 1..MaxLogLen
    /\ p.reqID # NoReq
    /\ p.term  >= glTerm
    /\ commitIndex + 1 >= i
    /\ DomainHoldsAt(d, i, p)
    /\ p.reqID \notin fastSent
    /\ fastSent'     = fastSent \cup {p.reqID}
    /\ clientResult' = [clientResult EXCEPT ![p.reqID] = p]
    /\ UNCHANGED << glLog, glTerm, commitIndex, appliedIndex, stateMachine,
                    resultsMap, dlog, pendingFR, everTwoHeld >>

AdvanceCommit ==
    /\ commitIndex < Len(glLog)
    /\ LET i == commitIndex + 1 IN
        /\ DomainHoldsAt(GlobalDomain, i, glLog[i])
        /\ \E d \in Domains \ {GlobalDomain}: DomainHoldsAt(d, i, glLog[i])
        /\ commitIndex' = i
        /\ resultsMap'  = [resultsMap EXCEPT ![glLog[i].reqID] = glLog[i]]
    /\ UNCHANGED << glLog, glTerm, appliedIndex, stateMachine,
                    dlog, pendingFR, fastSent, clientResult, everTwoHeld >>

ApplyOne ==
    /\ appliedIndex < commitIndex
    /\ LET i == appliedIndex + 1
           e == glLog[i] IN
        /\ stateMachine' = [stateMachine EXCEPT ![e.key] = e.val]
        /\ appliedIndex' = i
    /\ UNCHANGED << glLog, glTerm, commitIndex, resultsMap, dlog, pendingFR,
                    fastSent, clientResult, everTwoHeld >>

\* Preserve entries that previously obtained two-domain quorum evidence.
BumpTerm ==
    /\ glTerm + 1 <= MaxTerm
    /\ \E L \in commitIndex..Len(glLog):
         /\ \A e \in everTwoHeld:
              (e.idx \in 1..Len(glLog) /\ EntryEq(glLog[e.idx], e)) => e.idx <= L
         /\ glLog'   = SubSeq(glLog, 1, L)
         /\ glTerm'  = glTerm + 1
    /\ UNCHANGED << commitIndex, appliedIndex, stateMachine, resultsMap, dlog,
                    pendingFR, fastSent, clientResult, everTwoHeld >>

Next ==
    \/ \E r \in RequestIDs, k \in Keys, v \in Values: AppendEntry(r, k, v)
    \/ \E d \in Domains,    i \in 0..MaxLogLen:       DomainCatchUp(d, i)
    \/ \E d \in Domains,    i \in 1..MaxLogLen:       SendFRCertificate(d, i)
    \/ \E d \in Domains,    i \in 1..MaxLogLen:       DeliverFR(d, i)
    \/ AdvanceCommit
    \/ ApplyOne
    \/ BumpTerm

Spec == Init /\ [][Next]_vars

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

FastReturnSafety ==
    \A r \in fastSent:
        LET c == clientResult[r] IN
        /\ c.reqID = r
        /\ c.idx \in 1..Len(glLog)
        /\ EntryEq(glLog[c.idx], c)

FastReturnAgreesWithCommit ==
    \A r \in fastSent:
        LET c == clientResult[r] IN
        (c.idx <= commitIndex) =>
            /\ EntryEq(glLog[c.idx], c)
            /\ EntryEq(resultsMap[r], c)

DualIndexReadSafety ==
    LET barrier == DQI(GlobalDomain) IN
    (commitIndex >= barrier /\ appliedIndex >= barrier) =>
        \A k \in Keys:
            stateMachine[k] = LatestCommittedValue(k, appliedIndex)

StateMachineCoherent ==
    \A k \in Keys:
        stateMachine[k] = LatestCommittedValue(k, appliedIndex)

IndexOrderingInvariant ==
    /\ appliedIndex <= commitIndex
    /\ commitIndex  <= DQI(GlobalDomain)

\* Retained history remains consistent with glLog.
EverTwoHeldRespected ==
    \A e \in everTwoHeld:
        (e.idx \in 1..Len(glLog)) => EntryEq(glLog[e.idx], e)

================================================================================
