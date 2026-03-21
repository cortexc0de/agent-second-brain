# Founder Decision Partner V1

## Product Thesis

`Founder Decision Partner` is a Telegram-first decision assistant for founders, solo builders, and product leads.

It is not a generic second brain and not a general-purpose agent platform.

The V1 promise is narrow:

> Help the user decide what to do next out of chaos, with a strong recommendation for the next 7-14 days.

The product wins when the user says:

> "That is the right call. I am doing this."

## Target User

Primary users:

- founders
- solo builders
- product people with broad ownership

Shared traits:

- high information overload
- too many active directions
- incomplete systems of thought across chat, notes, and tasks
- strong need for clarity, prioritization, and decision pressure

## Core Use Case

Primary job to be done:

> "I have too many directions. Tell me what to focus on next and what to cut."

Included request types:

- prioritization
- choosing between 2-4 options
- stopping low-value work
- diagnosing why progress is stuck

Explicitly out of scope for V1:

- hiring decisions
- financial decisions
- complex GTM strategy
- broad workflow automation
- multi-channel collaboration

## Product Principles

1. The agent must take a position.
2. The agent must be willing to disagree.
3. The agent must reason over memory, not just the current message.
4. Every decision must have a review point.
5. V1 optimizes for wow-quality, not throughput.

## Interface

Primary interface:

- Telegram chat

Deferred:

- web app
- public API
- Slack
- CRM
- calendar
- analytics integrations

## Context Sources

V1 sources:

- vault
- Telegram dialogue history
- optional Todoist

Priority rule:

> one deep context source is better than many shallow ones

## Agent Stance

Tone:

- calm
- direct
- rational
- respectful

Not allowed:

- coaching tone
- "here are some options, you decide"
- empty reassurance
- harsh or performatively aggressive language

Preferred style:

> I would choose X and stop Y because the evidence points to A, while Y looks like a repeat of a previous unproductive loop.

## Decision Horizon

Default horizon:

- 14 days

Allowed shorter horizon:

- 7 days when the problem is highly tactical

Not default:

- same-day task advice
- long-range strategic planning beyond the evidence window

Product rule:

> advice should be framed as a testable 7-14 day commitment, not as a vague opinion

## Response Contract

Every strong response should follow this structure:

### Verdict

One explicit recommendation.

### Why

The strongest reasons, grounded in current context and historical evidence.

### Do Not Do

The specific directions, projects, or decisions the user should pause, reject, or delay.

### Risks

What could make the recommendation wrong.

### What To Check In 14 Days

A short validation loop with observable signals.

Example:

```text
Verdict:
Focus only on B2B for the next 14 days and freeze B2C.

Why:
- B2B already has signal
- B2C is consuming attention without proof
- this looks similar to earlier focus fragmentation

Do Not Do:
- do not run both tracks in parallel
- do not optimize B2C messaging yet

Risks:
- B2B signal may be weaker than it appears

What To Check In 14 Days:
- number of qualified conversations
- conversion to next-step commitments
- whether one repeatable message is emerging
```

## Decision Protocol

Each request should pass through this reasoning loop:

1. Classify the decision type.
2. Retrieve relevant context from vault and history.
3. Build a tension map.
4. Detect bias and repeated patterns.
5. Generate one recommendation.
6. Challenge the recommendation with a counter-argument.
7. Produce the response in the fixed contract.
8. Commit the decision and create a review checkpoint.

## Tension Map

The agent should explicitly reason about:

- what the user says they want
- what they are actually optimizing for
- what is causing overload
- where attention is split
- what constraints are real vs self-created

## Bias And Pattern Detection

This is core, not optional.

The agent should look for:

- novelty seeking
- premature pivoting
- unfinished loops
- overthinking without action
- risk avoidance disguised as strategy
- reactive work displacing important work

Examples of useful framing:

- "This looks like another early pivot before enough signal."
- "This sounds less like strategy and more like avoidance."
- "You have already tried this pattern multiple times with the same result."

## Review Loop

Every recommendation creates a review checkpoint.

Minimum review payload:

- decision taken
- expected outcome
- chosen time horizon
- review date

Review prompt example:

> Two weeks ago we chose X because of Y. Did the expected signal appear?

This turns advice into a learning loop.

## Memory Requirements

V1 must remember three classes of information:

1. Facts and decisions
2. Repeated behavioral patterns
3. Thinking style

Thinking style examples:

- avoids risk
- overcommits
- changes direction too early
- seeks ideal clarity before acting

## Success Criteria

Primary success metrics:

- user takes action based on the recommendation
- user returns for another decision
- user explicitly reports clarity or relief

Secondary metrics:

- review completion rate
- ratio of decisions with a clear verdict
- percentage of reviewed decisions judged useful

## Non-Goals

V1 is not trying to be:

- a general chat assistant
- an all-in-one productivity OS
- a workflow automation platform
- an enterprise collaboration suite
- a fully self-directed autonomous agent

## V1 Deliverable

The first release is successful if it can reliably do this:

> A founder sends a messy decision problem, and the system returns one sharp recommendation that feels better than thinking alone.
