
## 2026-07-06: Subagent model selection discipline
- Mistake: large review workflows and Explore/Plan agents ran without `model` overrides,
  inheriting the top session model for work that was mostly standard exploration.
- Rule (from user CLAUDE.md): finders/exploration → sonnet; mechanical sweeps → haiku;
  adversarial verification, final synthesis, security-sensitive judgment → opus/inherit.
- Apply: choose the model per dispatch, every time — including inside Workflow scripts
  (agent() takes model/effort opts); ultracode being on is not a reason to skip tiering.
