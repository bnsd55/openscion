The first PR of #961: the workflow alone, no runs yet.

**What it does**: a `workflow_dispatch` run of cargo-mutants over the files the dispatch names, each mutant's test run bounded by a timeout input, the outcome lists (missed, timeout, caught) uploaded as an artifact. The run happens on CI runners, so it never fills a developer's disk - the two local runs that caused disk emergencies are why this file exists.

**Shape**:
- `files` (required): space-separated repo-relative paths; each is checked to exist before anything runs, so a typo fails the dispatch instead of mutating nothing.
- `timeout_per_mutant` (default 120 s), `job_timeout_minutes` (default 120, wired into the job's `timeout-minutes`), `test_args` (default empty: the mutated package's whole test suite; the dispatch can narrow it, e.g. `--test access --test every_rpc_unauthenticated`).
- cargo-mutants is pinned (27.1.0 via install-action), so two runs mutate the same way; the outcome lists upload as an artifact, and the summary step FAILS if the lists are absent (a run that got past the build always writes them, so their absence is a bug, not "(none) missed").
- cargo-mutants tests the mutated file's PACKAGE by default, not the workspace: a mutant in scion-server runs open-scion-server's tests. With the default (whole package suite), the run time per mutant is the package suite's time - the scion-server suite alone is minutes, so per-mutant timeouts and the job budget must be chosen against that: for access.rs + views.rs the dispatch should narrow `test_args` to the suites that exercise them, or raise the timeouts. The 120-minute default fits a narrowed run; a whole-suite run needs the job budget raised with it.
- The tests run against the pg17 service the other workflows use (the auth and read-rule suites open the test database).
- No `pull_request` trigger on purpose: a mutation run is minutes per mutant, and no PR lane should pay for one - the same reason coverage, bench and soak are their own files. actionlint clean (the repo's lint gate).

**Where the tests ran**: actionlint over the new workflow and ci.yml, clean. No cargo-mutants run was made on any developer machine (the point of the change); the first run happens on CI after this lands.

Refs #961