use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_async_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "nextval_uses_sequence_adapter",
            description: "nextval should validate the sequence through the type-check adapter and build an async function.",
            setup_sqls: &[],
            sql: "nextval(seq)",
        },
        SqlTestCase {
            name: "dict_get_uses_dictionary_adapter",
            description: "dict_get should resolve dictionary metadata through the type-check adapter.",
            setup_sqls: &[],
            sql: "dict_get(dict, 'field', number)",
        },
        SqlTestCase {
            name: "read_file_uses_stage_adapter",
            description: "Two-argument read_file should resolve the stage through the type-check adapter.",
            setup_sqls: &[],
            sql: "read_file('@stage', 'file.txt')",
        },
        SqlTestCase {
            name: "read_file_rejects_invalid_arity",
            description: "read_file should validate its one-or-two argument shape during async-function lowering.",
            setup_sqls: &[],
            sql: "read_file()",
        },
        SqlTestCase {
            name: "read_file_requires_stage_path",
            description: "A single-argument read_file call should require an @stage path when the location is constant.",
            setup_sqls: &[],
            sql: "read_file('not_stage_path')",
        },
        SqlTestCase {
            name: "async_functions_cannot_be_nested",
            description: "Async functions should remain rejected while resolving another async function argument.",
            setup_sqls: &[],
            sql: "read_file(read_file('@s/file.txt'))",
        },
        SqlTestCase {
            name: "nextval_requires_identifier_argument",
            description: "nextval should reject non-identifier arguments before sequence resolution.",
            setup_sqls: &[],
            sql: "nextval(1)",
        },
        SqlTestCase {
            name: "nextval_rejects_multipart_identifier",
            description: "nextval should accept only a one-part sequence identifier.",
            setup_sqls: &[],
            sql: "nextval(db.seq)",
        },
        SqlTestCase {
            name: "dict_get_requires_dictionary_identifier",
            description: "dict_get should reject a non-identifier dictionary argument before catalog resolution.",
            setup_sqls: &[],
            sql: "dict_get('dict', 'field', number)",
        },
        SqlTestCase {
            name: "dict_get_rejects_three_part_identifier",
            description: "dict_get should accept only one-or-two-part dictionary identifiers.",
            setup_sqls: &[],
            sql: "dict_get(catalog.db.dict, 'field', number)",
        },
    ];

    run_type_check_cases("async_functions.txt", &cases).await
}
