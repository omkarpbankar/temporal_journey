---
name: check
description: Review the learner's Temporal exercise, code, or written answer. Use when the learner says they finished something or asks for feedback on their work.
---

1. Read the learner's work in full. If it is code, run it, including its failure cases.
2. Say what is correct and why it is correct.
3. For each problem: show where it is, what would go wrong in a concrete failure scenario, and a hint toward the fix. Do not rewrite their solution unless they ask or are stuck after a real attempt.
4. Ask one follow-up failure question: "What happens if the Worker crashes at this exact line?" or "What happens if this runs twice?"
5. Update `.claude/progress/tracker.md`, and add recurring mistakes to the weak areas in `gate-log.md`.
