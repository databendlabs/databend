# TLA+ models

Use the evot `tla-model` skill to create, change, run and triage these models; read its SKILL.md first.

- One directory per protocol: `<Module>.tla`, `model.json`, and `README.md` with the modeled logic and full bound source commit.
- Run: `python3 ~/.evotai/skills/tla-model/scripts/tlc_run.py tests/tla/<model>`. The installed skill is at `~/.evotai/skills/tla-model/`.
- Default is replay of the bound model: do not scan current source diffs, auto-correct the model, change its binding, or attribute results to incidental HEAD.
- Only an explicit request to validate the current branch/PR/latest code or source correspondence authorizes synchronization. Compare against the recorded commit, review callers/helpers and dirty files, then update the model and rerun every case. Keep source review separate from TLC results.
- Use the evot skill's replay/sync workflow; if the installed copy is older, use the updated evot-skills checkout. Do not add a Databend-local runner.
- Never commit generated `.cfg` files, traces, jars or logs. These checks are not part of CI.
