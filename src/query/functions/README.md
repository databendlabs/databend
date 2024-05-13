# How to add new functions

- [Scalar function](https://docs.databend.com/guides/community/contributor/how-to-write-scalar-functions)

- [Aggregate function](https://docs.databend.com/guides/community/contributor/how-to-write-aggregate-functions)

## Don't forget to add tests

- run:
	`env REGENERATE_GOLDENFILES=1 cargo test` after adding new functions

- build and run databend, prepare for logical tests:
	```shell
	make build
	make run-debug
	```

- Add logic tests file in "tests/sqllogictests/suites/query/functions/*.test" and run:
	```shell
	cargo run \
    -p databend-sqllogictests \
    --bin databend-sqllogictests \
    -- \
    --handlers "http" \
    --run_dir functions \
    --run_file <test_file_name> \
    --debug \
    --parallel 2
	```
