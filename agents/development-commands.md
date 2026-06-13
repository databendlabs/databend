# Build, Test, and Development Commands

- `make setup`: install cargo components plus linters such as `taplo`, `shfmt`, `typos`, `machete`, and `ruff`.
- `make build` / `make build-release`: compile debug or optimized `databend-{query,meta,metactl}` binaries into `target/`.
- `make run-debug` / `make kill`: launch or stop a local standalone deployment using the latest build.
- `make unit-test`, `make stateless-test`, `make sqllogic-test`, `make metactl-test`: run focused suites.
- `make test`: run the default CI test matrix.
- `make fmt`: apply repository formatting rules.
- `make lint`: run lint checks before committing.
