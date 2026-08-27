use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_vector_functions() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "vector_distance_without_index_rewrite_falls_back",
            description: "Vector distance functions should preserve the old scalar-function fallback when no vector index rewrite applies.",
            setup_sqls: &[],
            sql: "l2_distance([1, 2]::vector(2), [3, 4]::vector(2))",
        },
        SqlTestCase {
            name: "vector_distance_with_array_args_falls_back",
            description: "Vector distance functions over array arguments should still use regular scalar type checking.",
            setup_sqls: &[],
            sql: "cosine_distance([1, 0], [0, 1])",
        },
    ];

    run_type_check_cases("vector.txt", &cases).await
}
