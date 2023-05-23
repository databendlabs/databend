// Copyright 2021 Datafuse Labs
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

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::parquet::encoding::Encoding;
use common_expression::types::AnyType;
use common_expression::DataBlock;
use common_expression::Value;

pub fn get_encodings(block: &DataBlock, schema: &ArrowSchema) -> Vec<Encoding> {
    block
        .columns()
        .into_iter()
        .zip(schema.fields.iter())
        .map(|(column, field)| get_encoding(&column.value, &field.data_type))
        .collect()
}

/// Get the optimal encoding for this column.
///
/// Selects the best encoding based on the value's type and Arrow data type.
///
/// # Arguments
///
/// - `value`: A reference to the value of type `Value<AnyType>`, representing the value to be encoded.
/// - `data_type`: A reference to the Arrow data type for the column.
///
/// # Returns
///
/// The optimal encoding for the column.
fn get_encoding(value: &Value<AnyType>, data_type: &ArrowDataType) -> Encoding {
    let alter_encodings = get_alter_encodings(data_type);
    select_best_encoding(value, &alter_encodings)
}

/// Get all possible encodings based on the data type.
///
/// Returns a vector containing all possible encodings for the given Arrow data type.
///
/// # Arguments
///
/// - `data_type`: A reference to the Arrow data type.
///
/// # Returns
///
/// A vector of encodings.
fn get_alter_encodings(data_type: &ArrowDataType) -> Vec<Encoding> {
    let mut alter_encodings: Vec<Encoding> = vec![];
    match data_type {
        ArrowDataType::Dictionary(..) => {
            alter_encodings.push(Encoding::RleDictionary);
        }
        ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8 => {
            alter_encodings.push(Encoding::DeltaLengthByteArray);
        }
        ArrowDataType::Null
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Date32
        | ArrowDataType::Time32(_)
        | ArrowDataType::Int64
        | ArrowDataType::Date64
        | ArrowDataType::Time64(_)
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Duration(_) => {
            alter_encodings.push(Encoding::Plain);
            alter_encodings.push(Encoding::DeltaBinaryPacked);
        }
        _ => {
            alter_encodings.push(Encoding::Plain);
        }
    };
    alter_encodings
}

/// Select the best encoding from a list of encodings.
///
/// Returns the best encoding based on the provided value and the list of encodings.
///
/// # Arguments
///
/// - `value`: A reference to the value of type `Value<AnyType>`, representing the value to be encoded.
/// - `encodings`: A slice containing the available encodings.
///
/// # Returns
///
/// The best encoding.
fn select_best_encoding(value: &Value<AnyType>, encodings: &[Encoding]) -> Encoding {
    assert!(encodings.len() > 0);
    if encodings.len() == 1 {
        encodings[0]
    } else {
        let mut best_encoding = encodings[0];
        let mut best_score = usize::MAX;
        for encoding in encodings {
            let score = calculate_encoding_score(value, encoding);
            if score < best_score {
                best_score = score;
                best_encoding = *encoding;
            }
        }
        best_encoding
    }
}

fn calculate_encoding_score(value: &Value<AnyType>, encoding: &Encoding) -> usize {
    match encoding {
        // TODO complete calculate func
        Encoding::Plain => 0,
        Encoding::DeltaBinaryPacked => calculate_delta_binary_packed_score(value),
        _ => unreachable!(),
    }
}

// TODO complete
fn calculate_delta_binary_packed_score(value: &Value<AnyType>) -> usize {
    return 0;
}
