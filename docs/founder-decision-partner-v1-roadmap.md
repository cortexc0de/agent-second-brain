# Founder Decision Partner V1 Roadmap

## Goal

Ship a narrow V1 that proves one thing:

> the system can give a founder a better next decision than thinking alone

## Delivery Strategy

Optimize for:

- wow-quality over breadth
- one strong path over many weak capabilities
- evaluation and iteration over premature platform work

## Phase 0: Product Lock

Objective:

- stop product drift

Deliverables:

- V1 product spec
- technical spec
- response contract
- eval rubric

Exit criteria:

- product thesis is stable
- one killer flow is agreed
- out-of-scope list is explicit

## Phase 1: Decision MVP

Objective:

- deliver `/decide` end-to-end

Scope:

- Telegram intake
- decision classification
- vault/history retrieval
- structured recommendation output
- manual or simple persisted decision logging

Implementation tasks:

1. Create `Decision Service` boundary.
2. Add `/decide` command path.
3. Implement the fixed response contract.
4. Persist `decision_run` and `decision_record`.
5. Add trace capture for every run.

Exit criteria:

- user can ask one messy decision question
- system returns a strong recommendation
- decision is stored for later review

## Phase 2: Pattern Memory

Objective:

- make the assistant feel like it understands the user

Scope:

- detect repeated loops
- store and update `pattern_record`
- surface patterns in recommendation output when relevant

Implementation tasks:

1. Define pattern extraction rules.
2. Add `bias / pattern detection` stage to the pipeline.
3. Persist `pattern_record`.
4. Add confidence and evidence fields.

Exit criteria:

- responses can explicitly cite repeated behavior
- patterns are not generic, they are tied to evidence

## Phase 3: Review Loop

Objective:

- close the learning loop

Scope:

- create `review_record` automatically
- send a follow-up after 7-14 days
- capture actual outcome
- update decision and pattern memory

Implementation tasks:

1. Add scheduler/worker.
2. Mark reviews as `scheduled`, `due`, `completed`, `skipped`.
3. Implement Telegram review prompts.
4. Update decision outcomes from user replies.

Exit criteria:

- every V1 decision has a review path
- completed reviews improve later recommendations

## Phase 4: Optional Action Layer

Objective:

- let the user convert a decision into action

Scope:

- Todoist task creation from a final recommendation

Implementation tasks:

1. Add explicit "create follow-up task" action.
2. Keep it optional and narrow.
3. Do not expand into full workflow automation.

Exit criteria:

- decisions can become tasks without changing product focus

## Phase 5: Evaluation And Tuning

Objective:

- improve reasoning quality with evidence

Scope:

- curated evaluation set
- structured scoring
- prompt and retrieval tuning

Eval dimensions:

- clarity of verdict
- strength of recommendation
- evidence quality
- pattern relevance
- actionability
- felt understanding

Implementation tasks:

1. Build 30-50 founder prompts.
2. Score outputs consistently.
3. Compare revisions against the same set.
4. Track reviewed outcomes where possible.

Exit criteria:

- measurable improvement over baseline
- stable output quality on the core use case

## Suggested Backlog

### Sprint 1

- add V1 docs
- create `Decision Service`
- add `/decide`
- implement stable response contract
- persist basic decision records

### Sprint 2

- add structured traces
- add pattern extraction
- add pattern store
- surface pattern references in output

### Sprint 3

- add review scheduling
- add review prompts
- capture outcome and update records

### Sprint 4

- add Todoist follow-up task creation
- build eval set
- tune prompting and retrieval

## What Not To Build Early

- general multi-tenant platform abstractions
- public API product
- Slack and CRM integrations
- broad automation workflows
- long-range strategic planning engine
- enterprise-grade collaboration features

## Team Decision Rules

When uncertain, prefer the option that:

1. improves recommendation quality on `/decide`
2. strengthens pattern memory
3. makes review loop more reliable

Reject work that mainly improves architecture elegance without improving the core decision experience.
