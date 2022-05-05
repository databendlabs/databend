use std::sync::Arc;
use nom::AsBytes;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use databend_query::format::format_csv::CsvInputState;
use databend_query::format::format_csv::CsvInputFormat;
use databend_query::format::{InputFormat, InputState};

#[test]
fn test_accepted_multi_lines() -> Result<()> {
    assert_complete_line("")?;
    assert_complete_line("first\tsecond\n")?;
    assert_complete_line("first\tsecond\r")?;
    assert_complete_line("first\tsecond\r\n")?;
    assert_complete_line("first\tsecond\n\r")?;
    assert_complete_line("first\t\"\n\"second\n")?;
    assert_complete_line("first\t\"\r\"second\n")?;

    assert_broken_line("first", 5)?;
    assert_broken_line("first\t", 6)?;
    assert_broken_line("first\ts", 7)?;
    assert_broken_line("first\ts\"\n", 9)?;
    assert_broken_line("first\ts\"\r", 9)?;
    assert_broken_line("first\tsecond\ns", 13)?;

    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::empty()),
        FormatSettings::default(),
        2,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    let bytes = "first\tsecond\nfirst\t".as_bytes();
    assert_eq!(bytes.len(), csv_input_format.read_buf(bytes, &mut csv_input_state)?);
    assert_eq!(bytes, &csv_input_state.as_any().downcast_mut::<CsvInputState>().unwrap().memory);

    let bytes = "second\nfirst\t".as_bytes();
    assert_eq!(7, csv_input_format.read_buf(bytes, &mut csv_input_state)?);
    assert_eq!("first\tsecond\nfirst\tsecond\n".as_bytes(), csv_input_state.as_any().downcast_mut::<CsvInputState>().unwrap().memory);
    Ok(())
}

fn assert_complete_line(content: &str) -> Result<()> {
    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::empty()),
        FormatSettings::default(),
        1,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    let bytes = content.as_bytes();
    assert_eq!(bytes.len(), csv_input_format.read_buf(bytes, &mut csv_input_state)?);
    assert_eq!(bytes, &csv_input_state.as_any().downcast_mut::<CsvInputState>().unwrap().memory);
    Ok(())
}

fn assert_broken_line(content: &str, assert_size: usize) -> Result<()> {
    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::empty()),
        FormatSettings::default(),
        1,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    let bytes = content.as_bytes();
    assert_eq!(assert_size, csv_input_format.read_buf(bytes, &mut csv_input_state)?);
    assert_eq!(&bytes[0..assert_size], &csv_input_state.as_any().downcast_mut::<CsvInputState>().unwrap().memory);
    Ok(())
}
