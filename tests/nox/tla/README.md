# Local TLA+ checks

Shared runner and TLC dependency preparation for model directories containing `model.json` and `<Module>.tla`. Requirements: Python 3.10+, nox, Java 11+ (`PATH` or `JAVA_HOME`). No Databend build or running service is needed.

```sh
cd tests/nox
nox -s tla -- tla/result_spill --list
nox -s tla -- tla/result_spill
nox -s tla -- tla/result_spill --case batch_deadline
```
