use databend_common_sql::TypeChecker;

#[test]
fn test_is_sugar_function_match() {
    assert!(TypeChecker::is_sugar_function("database"));
    assert!(TypeChecker::is_sugar_function("DATABASE"));
    assert!(TypeChecker::is_sugar_function("version"));
    assert!(TypeChecker::is_sugar_function("VERSION"));
    assert!(TypeChecker::is_sugar_function("current_user"));
}
