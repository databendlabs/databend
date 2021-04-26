#[test]
fn test_data_block_kernel_take() {
    use crate::exception::*;

    assert_eq!(format!("{}", ErrorCodes::Ok(String::from("test message 1"))),
               String::from("Code: 0, displayText = test message 1."));

    assert_eq!(
        format!("{}", ErrorCodes::Ok(String::from("test message 2"))),
        String::from("Code: 0, displayText = test message 2.")
    );
    assert_eq!(
        format!(
            "{}",
            ErrorCodes::UnknownException(String::from("test message 1"))
        ),
        String::from("Code: 1000, displayText = test message 1.")
    );
    assert_eq!(
        format!(
            "{}",
            ErrorCodes::UnknownException(String::from("test message 2"))
        ),
        String::from("Code: 1000, displayText = test message 2.")
    );
}
