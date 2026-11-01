---
name: gate
description: Run the mastery gate for the current Temporal phase. Use when the learner asks to take the gate or test whether they can move to the next phase.
---

Run the gate from the phase's "Mastery gate" section in plan.md, covering all four parts:

1. Conceptual questions, asked one at a time. Ask for real examples, not definitions.
2. A coding exercise, which you run and test.
3. A failure scenario.
4. An architecture question.

Do not give answers or hints that decide the outcome during the gate.

Pass means all four parts are solid. On a pass, mark the phase `passed` in `tracker.md` and log it in `gate-log.md`.

For the Final Assessment, use `journey/final-assessment/` and check answers against `.claude/assessment/answer-key.md`. Offer it only after the capstone.

If any part fails, explain the gap, reteach it with a different example from the one used before, record the weak area, and retest it later. Never move on past a failed gate.
