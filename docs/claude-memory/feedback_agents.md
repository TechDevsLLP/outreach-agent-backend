---
name: Parallel subagent pattern for OutFlo
description: User prefers parallel frontend+backend subagents for large feature work
type: feedback
---

For large features touching both Convex backend and Next.js frontend, launch two agents in parallel: one for Convex files only, one for Next.js pages/components only. They touch different files so there's no collision risk.

**Why:** User explicitly asked for this to "complete this task faster and optimally."

**How to apply:** Split work cleanly — backend agent gets all convex/ files, frontend agent gets all app/ and components/ files. Brief each agent with complete file paths and function signatures so they don't need to ask questions.
