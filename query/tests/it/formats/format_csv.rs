// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_datablocks::assert_blocks_eq;
use common_datavalues::type_primitive::UInt32Type;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataTypeImpl;
use common_datavalues::StringType;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use databend_query::formats::format_csv::CsvInputFormat;
use databend_query::formats::format_csv::CsvInputState;

#[test]
fn test_accepted_multi_lines() -> Result<()> {
    assert_complete_line("")?;
    assert_complete_line("first,second\n")?;
    assert_complete_line("first,second\r")?;
    assert_complete_line("first,second\r\n")?;
    assert_complete_line("first,second\n\r")?;
    assert_complete_line("first,\"\n\"second\n")?;
    assert_complete_line("first,\"\r\"second\n")?;

    assert_broken_line("first", 5)?;
    assert_broken_line("first,", 6)?;
    assert_broken_line("first,s", 7)?;
    assert_broken_line("first,s\"\n", 9)?;
    assert_broken_line("first,s\"\r", 9)?;
    assert_broken_line("first,second\ns", 13)?;

    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::empty()),
        FormatSettings::default(),
        2,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    let bytes = "first,second\nfirst,".as_bytes();
    assert_eq!(
        bytes.len(),
        csv_input_format.read_buf(bytes, &mut csv_input_state)?
    );
    assert_eq!(
        bytes,
        &csv_input_state
            .as_any()
            .downcast_mut::<CsvInputState>()
            .unwrap()
            .memory
    );

    let bytes = "second\nfirst,".as_bytes();
    assert_eq!(7, csv_input_format.read_buf(bytes, &mut csv_input_state)?);
    assert_eq!(
        "first,second\nfirst,second\n".as_bytes(),
        csv_input_state
            .as_any()
            .downcast_mut::<CsvInputState>()
            .unwrap()
            .memory
    );
    Ok(())
}

#[test]
fn test_deserialize_multi_lines() -> Result<()> {
    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::new(vec![
            DataField::new("a", DataTypeImpl::UInt32(UInt32Type::default())),
            DataField::new("b", DataTypeImpl::String(StringType::default())),
        ])),
        FormatSettings::default(),
        1,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    csv_input_format.read_buf("1,\"second\"\n".as_bytes(), &mut csv_input_state)?;
    assert_blocks_eq(
        vec![
            "+---+--------+",
            "| a | b      |",
            "+---+--------+",
            "| 1 | second |",
            "+---+--------+",
        ],
        &[csv_input_format.deserialize_data(&mut csv_input_state)?],
    );

    let csv_input_format = CsvInputFormat::try_create(
        "csv",
        Arc::new(DataSchema::new(vec![
            DataField::new("a", DataTypeImpl::UInt32(UInt32Type::default())),
            DataField::new("b", DataTypeImpl::String(StringType::default())),
        ])),
        FormatSettings::default(),
        2,
        10 * 1024 * 1024,
    )?;

    let mut csv_input_state = csv_input_format.create_state();

    csv_input_format.read_buf("1,\"second\"\n".as_bytes(), &mut csv_input_state)?;
    assert_blocks_eq(
        vec![
            "+---+--------+",
            "| a | b      |",
            "+---+--------+",
            "| 1 | second |",
            "+---+--------+",
        ],
        &[csv_input_format.deserialize_data(&mut csv_input_state)?],
    );
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
    assert_eq!(
        bytes.len(),
        csv_input_format.read_buf(bytes, &mut csv_input_state)?
    );
    assert_eq!(
        bytes,
        &csv_input_state
            .as_any()
            .downcast_mut::<CsvInputState>()
            .unwrap()
            .memory
    );
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
    assert_eq!(
        assert_size,
        csv_input_format.read_buf(bytes, &mut csv_input_state)?
    );
    assert_eq!(
        &bytes[0..assert_size],
        &csv_input_state
            .as_any()
            .downcast_mut::<CsvInputState>()
            .unwrap()
            .memory
    );
    Ok(())
}
