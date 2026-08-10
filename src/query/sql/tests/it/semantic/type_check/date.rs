use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_type_check_date_rewrites() -> Result<()> {
    let cases = [
        SqlTestCase {
            name: "date_add_date_from_function_tests_binds",
            description: "The date_add(day, n, date) scalar-test shape should type check through date arithmetic rewrite.",
            setup_sqls: &[],
            sql: "date_add(day, delta, date)",
        },
        SqlTestCase {
            name: "date_sub_timestamp_from_function_tests_binds",
            description: "The date_sub(hour, n, timestamp) shape should type check as negative date arithmetic.",
            setup_sqls: &[],
            sql: "date_sub(hour, delta, ts)",
        },
        SqlTestCase {
            name: "date_diff_day_from_function_tests_binds",
            description: "The date_diff(day, start, end) scalar-test shape should resolve to diff_days.",
            setup_sqls: &[],
            sql: "date_diff(day, date, to_date('2024-12-31'))",
        },
        SqlTestCase {
            name: "date_between_epoch_from_function_tests_binds",
            description: "The date_between(epoch, start, end) scalar-test shape should resolve to between_epochs.",
            setup_sqls: &[],
            sql: "date_between(epoch, date, to_date('2024-12-31'))",
        },
        SqlTestCase {
            name: "date_trunc_month_from_sqllogictest_binds",
            description: "The date_trunc(month, ts) form should resolve through the date truncation rewrite path.",
            setup_sqls: &[],
            sql: "date_trunc(month, ts)",
        },
        SqlTestCase {
            name: "time_slice_from_sqllogictest_binds",
            description: "TIME_SLICE with unit and boundary arguments should type check through the dedicated rewrite.",
            setup_sqls: &[],
            sql: "time_slice(ts, 8, 'day', 'end')",
        },
        SqlTestCase {
            name: "last_day_month_from_function_tests_binds",
            description: "last_day(timestamp, month) should type check through the adjacent calendar rewrite.",
            setup_sqls: &[],
            sql: "last_day(ts, month)",
        },
        SqlTestCase {
            name: "previous_day_from_function_tests_binds",
            description: "previous_day(timestamp, monday) should resolve the weekday literal during type checking.",
            setup_sqls: &[],
            sql: "previous_day(ts, monday)",
        },
        SqlTestCase {
            name: "next_day_from_function_tests_binds",
            description: "next_day(timestamp, friday) should resolve the weekday literal during type checking.",
            setup_sqls: &[],
            sql: "next_day(ts, friday)",
        },
    ];

    run_type_check_cases("date.txt", &cases).await
}
