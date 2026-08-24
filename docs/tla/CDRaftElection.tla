----------------------------- MODULE CDRaftElection -----------------------------
(***************************************************************************)
(* CD-Raft two-tier election safety.                                       *)
(*                                                                         *)
(* Goal: at most one leader per term, at BOTH layers.                       *)
(*   - Domain layer:  at most one Domain Leader per (domain, domain-term).  *)
(*   - Global layer:  at most one Global Leader per global-term.            *)
(*                                                                         *)
(* The global election treats each DOMAIN as a voter: a domain's global     *)
(* vote is physically cast by one of its Domain-Leader nodes. Global        *)
(* uniqueness therefore reduces to (a) two global quorums of domains        *)
(* intersect in a domain, and (b) a domain grants at most one global vote   *)
(* per global term. Property (b) is the crux: a single domain must not      *)
(* produce two conflicting global votes (the Issue #3 class). We model it   *)
(* with the PerDomainOnce guard. The buggy configuration drops that guard;  *)
(* TLC then finds a reachable dual-Global-Leader state, exactly because a    *)
(* domain that has two simultaneous Domain Leaders (an old one at term t     *)
(* and a new one at term t+1 that has not stepped down) double-votes.        *)
(*                                                                         *)
(* Leases / read-serving windows are deliberately OUT of scope: this spec   *)
(* is about election (vote) safety, not about overlapping serving windows.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Domains,        \* participating domains
    Servers,        \* participating servers
    MaxDTerm,       \* domain-term bound
    MaxGTerm,       \* global-term bound
    PerDomainOnce   \* TRUE: enforce one global vote per domain per term

(***************************************************************************)
(* Topology. Each participating domain has three replicas, so any domain    *)
(* can transiently hold two Domain Leaders in different domain terms.       *)
(* Global quorum follows the paper threshold max(2, N-1): with two domains *)
(* it is 2-of-2, and with three domains it is any two domains.              *)
(***************************************************************************)
AllDomains == {"A", "B", "C"}
AllServers == {"a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3"}
Nil        == "none"

DomainOf(s) ==
    CASE s \in {"a1", "a2", "a3"} -> "A"
      [] s \in {"b1", "b2", "b3"} -> "B"
      [] s \in {"c1", "c2", "c3"} -> "C"

DomainServers(d) == { s \in Servers : DomainOf(s) = d }

ASSUME MaxDTerm \in Nat \ {0}
ASSUME MaxGTerm \in Nat \ {0}
ASSUME PerDomainOnce \in BOOLEAN
ASSUME Domains \subseteq AllDomains
ASSUME Cardinality(Domains) >= 2
ASSUME Servers \subseteq AllServers
ASSUME \A s \in Servers: DomainOf(s) \in Domains
ASSUME \A d \in Domains: Cardinality(DomainServers(d)) = 3

IsDomainQuorum(d, Q) ==
    /\ Q \subseteq DomainServers(d)
    /\ 2 * Cardinality(Q) > Cardinality(DomainServers(d))

IsGlobalQuorum(DS) ==
    /\ DS \subseteq Domains
    /\ Cardinality(DS) >=
          IF Cardinality(Domains) - 1 >= 2
          THEN Cardinality(Domains) - 1
          ELSE 2

VARIABLES
    \* Domain (per-domain Raft) layer.
    dTerm,           \* [Servers -> 0..MaxDTerm]   highest domain term seen
    dVotedFor,       \* [Servers -> Servers u {Nil}] domain vote in dTerm
    dRole,           \* [Servers -> {"F","C","L"}]
    dVotesGranted,   \* [Servers -> SUBSET Servers]  domain votes a candidate holds
    domainElections, \* set of [dom, leader, term] successful domain elections
    \* Global (cross-domain) layer.
    gTerm,           \* [Servers -> 0..MaxGTerm]
    gVotedFor,       \* [Servers -> Servers u {Nil}]
    gRole,           \* [Servers -> {"F","C","L"}]
    gVotesGranted,   \* [Servers -> SUBSET Domains]  global votes (by domain) held
    globalGrants,    \* set of [dom, cand, gterm]  every global vote cast
    globalElections  \* set of [leader, term] successful global elections

domainVars == << dTerm, dVotedFor, dRole, dVotesGranted, domainElections >>
globalVars == << gTerm, gVotedFor, gRole, gVotesGranted, globalGrants,
                 globalElections >>
vars       == << dTerm, dVotedFor, dRole, dVotesGranted, domainElections,
                 gTerm, gVotedFor, gRole, gVotesGranted, globalGrants,
                 globalElections >>

Init ==
    /\ dTerm           = [s \in Servers |-> 0]
    /\ dVotedFor       = [s \in Servers |-> Nil]
    /\ dRole           = [s \in Servers |-> "F"]
    /\ dVotesGranted   = [s \in Servers |-> {}]
    /\ domainElections = {}
    /\ gTerm           = [s \in Servers |-> 0]
    /\ gVotedFor       = [s \in Servers |-> Nil]
    /\ gRole           = [s \in Servers |-> "F"]
    /\ gVotesGranted   = [s \in Servers |-> {}]
    /\ globalGrants    = {}
    /\ globalElections = {}

(***************************************************************************)
(* Domain layer actions.                                                   *)
(***************************************************************************)

\* A server starts a domain election: bump term, vote for self, campaign.
DStartElection(s) ==
    /\ dTerm[s] + 1 <= MaxDTerm
    /\ dTerm'         = [dTerm         EXCEPT ![s] = dTerm[s] + 1]
    /\ dVotedFor'     = [dVotedFor     EXCEPT ![s] = s]
    /\ dRole'         = [dRole         EXCEPT ![s] = "C"]
    /\ dVotesGranted' = [dVotesGranted EXCEPT ![s] = {s}]
    /\ UNCHANGED << domainElections >>
    /\ UNCHANGED globalVars

\* Another same-domain server grants its domain vote to candidate cand.
DGrantVote(voter, cand) ==
    /\ voter # cand
    /\ DomainOf(voter) = DomainOf(cand)
    /\ dRole[cand] = "C"
    /\ LET t == dTerm[cand] IN
        /\ t >= dTerm[voter]
        /\ (t > dTerm[voter] \/ dVotedFor[voter] = Nil)
        /\ dTerm'         = [dTerm         EXCEPT ![voter] = t]
        /\ dVotedFor'     = [dVotedFor     EXCEPT ![voter] = cand]
        /\ dRole'         = [dRole         EXCEPT ![voter] = "F"]
        /\ dVotesGranted' = [dVotesGranted EXCEPT ![cand]  = @ \cup {voter}]
    /\ UNCHANGED << domainElections >>
    /\ UNCHANGED globalVars

\* A candidate with a domain quorum becomes the Domain Leader.
DBecomeLeader(s) ==
    /\ dRole[s] = "C"
    /\ IsDomainQuorum(DomainOf(s), dVotesGranted[s])
    /\ dRole'           = [dRole EXCEPT ![s] = "L"]
    /\ domainElections' = domainElections \cup
                            {[dom |-> DomainOf(s), leader |-> s, term |-> dTerm[s]]}
    /\ UNCHANGED << dTerm, dVotedFor, dVotesGranted >>
    /\ UNCHANGED globalVars

(***************************************************************************)
(* Global layer actions. Only a Domain Leader may run or vote, because a    *)
(* domain participates in the global election through its leader.           *)
(***************************************************************************)

DomainAlreadyVoted(d, gt) ==
    \E r \in globalGrants : r.dom = d /\ r.gterm = gt

\* A Domain Leader runs for Global Leader; its own domain votes for it.
GStartElection(s) ==
    /\ dRole[s] = "L"
    /\ gTerm[s] + 1 <= MaxGTerm
    /\ LET gt == gTerm[s] + 1 IN
        /\ (PerDomainOnce => ~DomainAlreadyVoted(DomainOf(s), gt))
        /\ gTerm'         = [gTerm         EXCEPT ![s] = gt]
        /\ gVotedFor'     = [gVotedFor     EXCEPT ![s] = s]
        /\ gRole'         = [gRole         EXCEPT ![s] = "C"]
        /\ gVotesGranted' = [gVotesGranted EXCEPT ![s] = {DomainOf(s)}]
        /\ globalGrants'  = globalGrants \cup
                              {[dom |-> DomainOf(s), cand |-> s, gterm |-> gt]}
    /\ UNCHANGED << globalElections >>
    /\ UNCHANGED domainVars

\* A Domain Leader of another domain grants its domain's global vote to cand.
GGrantVote(voter, cand) ==
    /\ voter # cand
    /\ DomainOf(voter) # DomainOf(cand)
    /\ dRole[voter] = "L"
    /\ gRole[cand] = "C"
    /\ LET gt == gTerm[cand] IN
        /\ gt >= gTerm[voter]
        /\ (gt > gTerm[voter] \/ gVotedFor[voter] = Nil)
        /\ (PerDomainOnce => ~DomainAlreadyVoted(DomainOf(voter), gt))
        /\ gTerm'         = [gTerm         EXCEPT ![voter] = gt]
        /\ gVotedFor'     = [gVotedFor     EXCEPT ![voter] = cand]
        /\ gRole'         = [gRole         EXCEPT ![voter] = "F"]
        /\ gVotesGranted' = [gVotesGranted EXCEPT ![cand]  = @ \cup {DomainOf(voter)}]
        /\ globalGrants'  = globalGrants \cup
                              {[dom |-> DomainOf(voter), cand |-> cand, gterm |-> gt]}
    /\ UNCHANGED << globalElections >>
    /\ UNCHANGED domainVars

\* A candidate with a global quorum of domains becomes the Global Leader.
GBecomeLeader(s) ==
    /\ gRole[s] = "C"
    /\ IsGlobalQuorum(gVotesGranted[s])
    /\ gRole'           = [gRole EXCEPT ![s] = "L"]
    /\ globalElections' = globalElections \cup
                            {[leader |-> s, term |-> gTerm[s]]}
    /\ UNCHANGED << gTerm, gVotedFor, gVotesGranted, globalGrants >>
    /\ UNCHANGED domainVars

Next ==
    \/ \E s \in Servers: DStartElection(s)
    \/ \E v, c \in Servers: DGrantVote(v, c)
    \/ \E s \in Servers: DBecomeLeader(s)
    \/ \E s \in Servers: GStartElection(s)
    \/ \E v, c \in Servers: GGrantVote(v, c)
    \/ \E s \in Servers: GBecomeLeader(s)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants.                                                             *)
(***************************************************************************)

TypeOK ==
    /\ dTerm         \in [Servers -> 0..MaxDTerm]
    /\ dVotedFor     \in [Servers -> Servers \cup {Nil}]
    /\ dRole         \in [Servers -> {"F", "C", "L"}]
    /\ dVotesGranted \in [Servers -> SUBSET Servers]
    /\ gTerm         \in [Servers -> 0..MaxGTerm]
    /\ gVotedFor     \in [Servers -> Servers \cup {Nil}]
    /\ gRole         \in [Servers -> {"F", "C", "L"}]
    /\ gVotesGranted \in [Servers -> SUBSET Domains]
    /\ domainElections \subseteq [dom: Domains, leader: Servers, term: 1..MaxDTerm]
    /\ globalGrants    \subseteq [dom: Domains, cand: Servers, gterm: 1..MaxGTerm]
    /\ globalElections \subseteq [leader: Servers, term: 1..MaxGTerm]

\* At most one Domain Leader per (domain, domain-term).
DomainLeaderUniqueness ==
    \A e1, e2 \in domainElections:
        (e1.dom = e2.dom /\ e1.term = e2.term) => e1.leader = e2.leader

\* A domain casts at most one global vote per global term (the key lemma).
OneGlobalVotePerDomainPerTerm ==
    \A r1, r2 \in globalGrants:
        (r1.dom = r2.dom /\ r1.gterm = r2.gterm) => r1.cand = r2.cand

\* At most one Global Leader per global term (the headline property: no dual GL).
GlobalLeaderUniqueness ==
    \A e1, e2 \in globalElections:
        e1.term = e2.term => e1.leader = e2.leader

================================================================================
