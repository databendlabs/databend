//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::parquet::encoding::Encoding;

/// mapping from arrow DataType to Encoding
///
/// QUOTE: (From arrow2::parquet::write)
/// > Note that this is whether this implementation supports it, which is a subset of
/// > what the parquet spec allows.
/// ~~~ignore
/// pub fn can_encode(data_type: &DataType, encoding: Encoding) -> bool {
///    matches!(
///    (encoding, data_type),
///    (Encoding::Plain, _)
///        | (
///            Encoding::DeltaLengthByteArray,
///            DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8,
///        )
///        | (Encoding::RleDictionary, DataType::Dictionary(_, _))
///        | (Encoding::PlainDictionary, DataType::Dictionary(_, _))
///     )
///  }
///  ~~~
///
///
pub fn col_encoding(data_type: &ArrowDataType) -> Encoding {
    // Although encoding does work, parquet2 has not implemented decoding of DeltaLengthByteArray for Binary yet, we fallback to Plain
    // From parquet2 error message : Decoding "DeltaLengthByteArray"-encoded required V2 pages is not yet implemented for Binary.

    match data_type {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
        ArrowDataType::Dictionary(_, _) => Encoding::RleDictionary,
        _ => Encoding::Plain,
    }
}
