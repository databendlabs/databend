# How to add new functions

- [Scalar function](https://docs.databend.com/guides/community/contributor/how-to-write-scalar-functions)

- [Aggregate function](https://docs.databend.com/guides/community/contributor/how-to-write-aggregate-functions)


# Don't forget to add tests:
- run:
	`env REGENERATE_GOLDENFILES=1 cargo test` after adding new functions
- Add logic tests in "tests/sqllogictests/suites/query/02_function" and run:
	`cargo run -p databend-sqllogictests -- --handlers http --run_file <file_name>`
