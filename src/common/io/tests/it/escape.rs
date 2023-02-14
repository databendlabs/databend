use common_exception::Result;
use common_io::escape_string;

#[test]
fn test_escape() -> Result<()> {
    assert_eq!(escape_string("\t"), r#"\t"#);
    // '0x20' is space
    assert_eq!(escape_string("\x00_\x1F_ "), r#"\x00_\x1F_ "#);

    assert_eq!(escape_string("\\"), r#"\\"#);

    assert_eq!(escape_string("\'"), r#"\'"#);

    assert_eq!(escape_string("\""), "\\\"");
    Ok(())
}
