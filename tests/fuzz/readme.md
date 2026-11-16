# Fuzz and metamorphic tests

## `tlp.py` — metamorphic oracle testing (runs in CI)

Generates random predicates over small tables and checks identities that must hold for
every predicate, so a mismatch is a bug without a reference database:

- **TLP** (ternary logic partitioning): `Q == Q WHERE p ∪ Q WHERE NOT p ∪ Q WHERE p IS NULL`
  as multisets — filter push-down, join rewrites, NULL semantics.
- **NoREC**: `count(*) WHERE p == sum(CASE WHEN p THEN 1 ELSE 0 END)` — the right side
  evaluates `p` as a projection, bypassing filter push-down, pruning and prewhere.
- **Join TLP**: `INNER JOIN ON p == CROSS JOIN WHERE p`; TLP over a LEFT JOIN.
- **Aggregation**: DISTINCT vs GROUP BY group counts, partition counts add up.

Only internal errors (`Internal`, `PanicError`, `StorageNotFound`) and inconsistencies fail
the run; a query the server rejects with a semantic/type error is skipped. Everything is
deterministic per seed:

```shell
# against a running standalone query node (http handler on 8000)
python3 tests/fuzz/tlp.py --iterations 400 --seed 42
```

A failure prints the seed and the exact SQL to reproduce. Bugs it found on first use are
kept as regression tests in `tests/sqllogictests/suites/query/filter_semantics.test`.

## `fuzz.py` — grammar-based fuzzing (manual)

Generates SQL from a grammar and executes it through the MySQL handler; a result that is not
a MySQL error or `None` is a failure. Needs `pip3 install fuzzingbook mysql-connector`.

To add a grammar: define it (e.g. `select_grammar: Grammar = {}`), assert
`is_valid_grammar(select_grammar)`, and add it to `generator_list` with a fuzz count.
