# Scalar Function Golden Tests

Use this guide for changes under `src/query/functions/tests/it/scalars/`.

## What This Harness Covers

- `run_ast` and `run_ast_with_context` parse, type-check, fold with domains, evaluate, and write the result to a golden file.
- Golden output records the raw expression, checked expression, optimized expression when it changes, input domains, output domain, row results, and internal column output.
- Use these tests for scalar function behavior and function-level domain folding regressions.

## How To Add Cases

- Add ordinary scalar cases with `run_ast(file, "...", columns)`.
- Use `run_ast_with_context` when a case needs a custom `FunctionContext`, custom `input_domains`, or non-default evaluation strictness.
- Use `TestContext.input_domains` when the domain under test is different from the concrete row values.
- Keep the case close to the function group that owns it, for example modulo cases in `test_modulo`.

## Updating Goldens

- Regenerate with `env REGENERATE_GOLDENFILES=1 cargo test -p databend-common-functions --test it <test_name>`.
- Review the generated golden diff as part of the change.
