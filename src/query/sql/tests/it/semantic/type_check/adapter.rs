use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_adapter_mock_paths() -> Result<()> {
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
    ];

    run_type_check_cases("adapter.txt", &cases).await
}
