// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::ArcColumnWrapper;
use opensrv_clickhouse::types::column::ColumnFrom;
use serde_json::Value;
use streaming_iterator::StreamingIterator;

use crate::formats::iterators::new_it;
use crate::formats::iterators::NullInfo;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct StringSerializer {}

impl TypeSerializer for StringSerializer {
    fn serialize_value(&self, value: &DataValue, _format: &FormatSettings) -> Result<String> {
        if let DataValue::String(x) = value {
            Ok(String::from_utf8_lossy(x).to_string())
        } else {
            Err(ErrorCode::BadBytes("Incorrect String value"))
        }
    }

    fn serialize_column(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<Vec<String>> {
        let column: &StringColumn = Series::check_get(column)?;
        let result: Vec<String> = column
            .iter()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .collect();
        Ok(result)
    }

    fn serialize_column_quoted(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<Vec<String>> {
        let column: &StringColumn = Series::check_get(column)?;
        let result: Vec<String> = column
            .iter()
            .map(|v| format!("{:?}", String::from_utf8_lossy(v)))
            .collect();
        Ok(result)
    }

    fn serialize_json(&self, column: &ColumnRef, _format: &FormatSettings) -> Result<Vec<Value>> {
        let column: &StringColumn = Series::check_get(column)?;
        let result: Vec<Value> = column
            .iter()
            .map(|x| serde_json::to_value(String::from_utf8_lossy(x).to_string()).unwrap())
            .collect();
        Ok(result)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let column: &StringColumn = Series::check_get(column)?;
        let values: Vec<&[u8]> = column.iter().collect();
        Ok(Vec::column_from::<ArcColumnWrapper>(values))
    }

    fn serialize_json_object(
        &self,
        column: &ColumnRef,
        valids: Option<&Bitmap>,
        _format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        let column: &StringColumn = Series::check_get(column)?;
        let mut result: Vec<Value> = Vec::new();
        for (i, v) in column.iter().enumerate() {
            if let Some(valids) = valids {
                if !valids.get_bit(i) {
                    result.push(Value::Null);
                    continue;
                }
            }
            match std::str::from_utf8(v) {
                Ok(v) => match serde_json::from_str::<Value>(v) {
                    Ok(v) => result.push(v),
                    Err(e) => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "Error parsing JSON: {}",
                            e
                        )))
                    }
                },
                Err(e) => {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "Error parsing JSON: {}",
                        e
                    )))
                }
            }
        }
        Ok(result)
    }

    fn serialize_json_object_suppress_error(
        &self,
        column: &ColumnRef,
        _format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        let column: &StringColumn = Series::check_get(column)?;
        let result: Vec<Option<Value>> = column
            .iter()
            .map(|v| match std::str::from_utf8(v) {
                Ok(v) => match serde_json::from_str::<Value>(v) {
                    Ok(v) => Some(v),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect();
        Ok(result)
    }

    fn serialize_csv_inner<'a, F2>(
        &self,
        column: &'a ColumnRef,
        _format: &FormatSettings,
        nullable: NullInfo<F2>,
    ) -> Result<Box<dyn StreamingIterator<Item = [u8]> + 'a>>
    where
        F2: Fn(usize) -> bool + 'a,
    {
        let column2: &StringColumn = Series::check_get(&column)?;
        Ok(new_it(
            column2.iter(),
            |x, buf| buf.extend_from_slice(x),
            vec![],
            nullable,
        ))
    }

    fn write_csv_field_not_null<'a>(
        &self,
        column: &ColumnRef,
        row_num: usize,
        buf: &mut Vec<u8>,
        _format: &FormatSettings,
    ) -> Result<()> {
        let col: &<Vec<u8> as Scalar>::ColumnType = unsafe { Series::static_cast(&column) };
        buf.extend_from_slice(col.get_data(row_num));
        Ok(())
    }

    //  move it to DataType later
    fn get_csv_serializer<'a>(&self, column: &'a ColumnRef) -> Result<Box<dyn ColSerializer + 'a>> {
        let col: &StringColumn = Series::check_get(&column)?;
        let s = StringColSerializer { col };
        Ok(Box::new(s))
    }
}

#[derive(Clone)]
pub struct StringColSerializer<'a> {
    pub(crate) col: &'a StringColumn,
}

impl<'a> ColSerializer for StringColSerializer<'a> {
    fn write_csv_field(
        &self,
        row_num: usize,
        buf: &mut Vec<u8>,
        _format: &FormatSettings,
    ) -> Result<()> {
        buf.extend_from_slice(unsafe { self.col.value_unchecked(row_num) });
        Ok(())
    }
}
