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

//! Builds encoded in-memory columns (see `enable_encoded_columns`) from deserialized Parquet data.

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::DictionaryArray;
use arrow_array::types::UInt32Type;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DictColumn;
use databend_common_expression::types::EncodedColumn;
use databend_common_expression::types::ForColumn;
use databend_common_expression::types::NarrowBuffer;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnEncoding {
    /// Dictionary encoding, for types without an order-preserving integer form (e.g. `String`,
    /// floats). `String` columns reuse the Parquet dictionary.
    Dictionary,
    /// Frame of reference for integer-like values (integers, `Date`, `Timestamp`, `Decimal`),
    /// or dictionary encoding when the block has few distinct values over a wide range.
    FrameOfReference,
}

impl ColumnEncoding {
    /// Nullable strings keep their validity outside the dictionary encoded values.
    pub fn for_type(data_type: &DataType) -> Option<ColumnEncoding> {
        match data_type {
            DataType::Nullable(inner) if inner.is_string() => Some(ColumnEncoding::Dictionary),
            // 1-byte values cannot get narrower.
            DataType::Number(ty) if ty.bit_width() == 8 => None,
            // Frame of reference handles raw values up to `i128`.
            DataType::Decimal(size) if !size.can_carried_by_128() => {
                Some(ColumnEncoding::Dictionary)
            }
            ty if ForColumn::supports(ty) => Some(ColumnEncoding::FrameOfReference),
            ty if DictColumn::supports(ty) => Some(ColumnEncoding::Dictionary),
            _ => None,
        }
    }

    /// Whether the Parquet reader is asked to keep the dictionary of the column chunk, i.e. to
    /// return a `DictionaryArray` instead of expanding it. arrow-rs only preserves the Parquet
    /// dictionary for byte arrays; it would rebuild one for primitive types.
    pub fn reuses_parquet_dictionary(self, data_type: &DataType) -> bool {
        self == ColumnEncoding::Dictionary && data_type.remove_nullable().is_string()
    }

    /// Suffix of the column array cache key: encoded and decoded arrays of the same column
    /// are different cache entries.
    pub fn cache_type_key(encoding: Option<ColumnEncoding>, data_type: &DataType) -> String {
        match encoding {
            Some(ColumnEncoding::Dictionary) => format!("{data_type}#dict"),
            Some(ColumnEncoding::FrameOfReference) => format!("{data_type}#for"),
            None => data_type.to_string(),
        }
    }

    /// Converts the Arrow array read from Parquet into the narrowest in-memory form: the
    /// encoded column, or the regular column if no encoding makes the block smaller.
    pub fn encode(
        self,
        array: ArrayRef,
        data_type: &DataType,
        max_ndv_ratio: u64,
    ) -> Result<Column> {
        let inner_type = data_type.remove_nullable();
        if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<UInt32Type>>()
            && dict.values().null_count() == 0
            && (dict.null_count() == 0 || data_type.is_nullable())
        {
            if dict.values().is_empty()
                || dict.values().len() as u64 * 100 > dict.len() as u64 * max_ndv_ratio
            {
                return Column::from_arrow_rs(array, data_type);
            }
            // The Parquet dictionary, as is.
            let values = Column::from_arrow_rs(dict.values().clone(), &inner_type)?;
            let max_key = values.len().saturating_sub(1) as u64;
            // Null rows still need an in-bounds key for kernels that read the inner column.
            let keys = if dict.null_count() > 0 {
                NarrowBuffer::pack(
                    dict.keys().iter().map(|key| key.unwrap_or(0) as u64),
                    max_key,
                )
            } else {
                NarrowBuffer::pack(dict.keys().values().iter().map(|key| *key as u64), max_key)
            };
            let column = encoded(EncodedColumn::Dict(DictColumn::new(keys, values)));
            return Ok(if data_type.is_nullable() {
                let validity = match dict.nulls() {
                    Some(nulls) => Bitmap::from_null_buffer(nulls.clone()),
                    None => Bitmap::new_constant(true, dict.len()),
                };
                column.wrap_nullable(Some(validity))
            } else {
                column
            });
        }

        let column = Column::from_arrow_rs(array, data_type)?;
        Ok(match column {
            Column::Nullable(column) => {
                let (column, validity) = column.destructure();
                self.encode_column(column, max_ndv_ratio)
                    .wrap_nullable(Some(validity))
            }
            column => self.encode_column(column, max_ndv_ratio),
        })
    }

    fn encode_column(self, column: Column, max_ndv_ratio: u64) -> Column {
        let mut best = None;
        if self == ColumnEncoding::FrameOfReference
            && let Some(for_column) = ForColumn::try_encode(&column)
        {
            // Deltas of at most 2 bytes: a dictionary cannot be meaningfully narrower.
            if for_column.deltas().byte_width() <= 2 {
                return encoded(EncodedColumn::For(for_column));
            }
            best = Some(EncodedColumn::For(for_column));
        }
        let max_ndv = (column.len() as u64 * max_ndv_ratio / 100) as usize;
        if let Some(dict) = DictColumn::try_encode(&column, max_ndv) {
            let dict = EncodedColumn::Dict(dict);
            if best
                .as_ref()
                .is_none_or(|best| dict.memory_size(false) < best.memory_size(false))
            {
                best = Some(dict);
            }
        }
        match best {
            Some(best) if best.memory_size(false) < column.memory_size(false) => encoded(best),
            _ => column,
        }
    }
}

fn encoded(column: EncodedColumn) -> Column {
    Column::Encoded(Box::new(column))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Date32Array;
    use arrow_array::Decimal64Array;
    use arrow_array::Float64Array;
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_array::StringArray;
    use arrow_array::UInt32Array;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    /// Encodes `array` with the default dictionary NDV ratio, see [`encode_checked_with`].
    fn encode_checked(encoding: ColumnEncoding, array: ArrayRef, data_type: &DataType) -> Column {
        encode_checked_with(encoding, array, data_type, 100)
    }

    /// Encodes `array`, checks that the result decodes to the regular column and returns it.
    fn encode_checked_with(
        encoding: ColumnEncoding,
        array: ArrayRef,
        data_type: &DataType,
        max_ndv_ratio: u64,
    ) -> Column {
        let plain = Column::from_arrow_rs(array.clone(), data_type).unwrap();
        let column = encoding.encode(array, data_type, max_ndv_ratio).unwrap();
        let decoded = BlockEntry::Column(column.clone()).decoded_value();
        // The inner value of a NULL row is unspecified (encoded rows use an in-bounds
        // dictionary key), so compare logical values rather than placeholder bytes.
        assert!(decoded.as_column().unwrap().iter().eq(plain.iter()));
        assert_eq!(column.data_type(), *data_type);
        column
    }

    fn as_for(column: &Column) -> &ForColumn {
        match column {
            Column::Encoded(encoded) => encoded.as_for().expect("frame of reference column"),
            column => panic!("expected an encoded column, got {column:?}"),
        }
    }

    fn as_dict(column: &Column) -> &DictColumn {
        match column {
            Column::Encoded(encoded) => encoded.as_dict().expect("dictionary column"),
            column => panic!("expected an encoded column, got {column:?}"),
        }
    }

    fn int64() -> DataType {
        DataType::Number(NumberDataType::Int64)
    }

    #[test]
    fn test_encode_nullable_string_dictionary() {
        let data_type = DataType::Nullable(Box::new(DataType::String));
        assert_eq!(
            ColumnEncoding::for_type(&data_type),
            Some(ColumnEncoding::Dictionary)
        );
        assert!(ColumnEncoding::Dictionary.reuses_parquet_dictionary(&data_type));
        for all_valid in [false, true] {
            let values =
                StringArray::from(vec!["xxx/long/repeated/url", "yyy/another/url", "unused"]);
            let keys = UInt32Array::from(
                (0..1000)
                    .map(|i| (all_valid || i % 5 != 0).then_some(i % 2))
                    .collect::<Vec<_>>(),
            );
            let array = DictionaryArray::<UInt32Type>::try_new(keys, Arc::new(values)).unwrap();
            let column = encode_checked(
                ColumnEncoding::Dictionary,
                Arc::new(array.slice(3, 990)),
                &data_type,
            );
            let nullable = column.as_nullable().unwrap();
            assert!(nullable.column().is_encoded());
            assert_eq!(
                nullable.validity().null_count(),
                if all_valid { 0 } else { 198 }
            );
        }
        // Without a Parquet dictionary, the strings are dictionary encoded inside the validity.
        let array = StringArray::from(
            (0..1000)
                .map(|i| (i % 5 != 0).then_some("xxx/long/repeated/url"))
                .collect::<Vec<_>>(),
        );
        let column = encode_checked(ColumnEncoding::Dictionary, Arc::new(array), &data_type);
        let nullable = column.as_nullable().unwrap();
        assert!(nullable.column().is_encoded());
        assert_eq!(nullable.validity().null_count(), 200);

        // Nullable dictionary entries contribute logical nulls even when all keys are valid.
        let array = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from((0..1000).map(|i| i % 2).collect::<Vec<_>>()),
            Arc::new(StringArray::from(vec![Some("xxx/long/repeated/url"), None])),
        )
        .unwrap();
        let column = encode_checked(ColumnEncoding::Dictionary, Arc::new(array), &data_type);
        assert_eq!(column.as_nullable().unwrap().validity().null_count(), 500);

        // Empty dictionaries with all-null keys must not produce out-of-bounds codes.
        let array = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(vec![None; 16]),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        )
        .unwrap();
        let column = encode_checked(ColumnEncoding::Dictionary, Arc::new(array), &data_type);
        assert_eq!(column.as_nullable().unwrap().validity().null_count(), 16);
    }

    #[test]
    fn test_encode_parquet_dictionary() {
        // The dictionary returned by the Parquet reader is kept as is, unused entries included.
        let values = StringArray::from(vec!["A", "N", "R", "unused"]);
        let keys = UInt32Array::from((0..1000).map(|i| i % 3).collect::<Vec<u32>>());
        let array = DictionaryArray::<UInt32Type>::try_new(keys, Arc::new(values)).unwrap();
        let column = encode_checked(
            ColumnEncoding::Dictionary,
            Arc::new(array),
            &DataType::String,
        );

        let dict = as_dict(&column);
        assert_eq!(dict.values.len(), 4);
        assert_eq!(dict.keys.byte_width(), 1);

        // More dictionary values than the ratio allows: the regular column is kept.
        let values = StringArray::from((0..600).map(|i| i.to_string()).collect::<Vec<_>>());
        let keys = UInt32Array::from((0..1000).map(|i| i % 600).collect::<Vec<u32>>());
        let array = DictionaryArray::<UInt32Type>::try_new(keys, Arc::new(values)).unwrap();
        let column = encode_checked_with(
            ColumnEncoding::Dictionary,
            Arc::new(array),
            &DataType::String,
            50,
        );
        assert!(matches!(column, Column::String(_)));
    }

    #[test]
    fn test_encode_string_without_parquet_dictionary() {
        // A string column chunk without a dictionary page gets a dictionary built from it,
        // within the ratio.
        for (ratio, encoded) in [(100, true), (1, true), (0, false)] {
            let array = StringArray::from((0..1000).map(|i| ["x", "y"][i % 2]).collect::<Vec<_>>());
            let column = encode_checked_with(
                ColumnEncoding::Dictionary,
                Arc::new(array),
                &DataType::String,
                ratio,
            );
            assert_eq!(column.is_encoded(), encoded, "ratio {ratio}");
        }

        // All values distinct: the dictionary would be larger, so the regular column is kept.
        let array = StringArray::from((0..1000).map(|i| i.to_string()).collect::<Vec<_>>());
        let column = encode_checked(
            ColumnEncoding::Dictionary,
            Arc::new(array),
            &DataType::String,
        );
        assert!(matches!(column, Column::String(_)));
    }

    #[test]
    fn test_encode_narrow_range() {
        // A narrow value range takes the frame of reference directly.
        let array = Int64Array::from_iter_values((0..1000).map(|i| 1_000_000 + i % 200));
        let column = encode_checked(ColumnEncoding::FrameOfReference, Arc::new(array), &int64());
        let for_column = as_for(&column);
        assert_eq!(for_column.base(), 1_000_000);
        assert_eq!(for_column.deltas().byte_width(), 1);

        // Negative values.
        let array = Int32Array::from_iter_values((0..1000).map(|i| -30_000 + i * 50));
        let data_type = DataType::Number(NumberDataType::Int32);
        let column = encode_checked(
            ColumnEncoding::FrameOfReference,
            Arc::new(array),
            &data_type,
        );
        let for_column = as_for(&column);
        assert_eq!(for_column.base(), -30_000);
        assert_eq!(for_column.deltas().byte_width(), 2);
    }

    #[test]
    fn test_encode_wide_range_picks_the_smallest() {
        // 4-byte deltas and many distinct values: the frame of reference.
        let array = Int64Array::from_iter_values((0..1000).map(|i| i * 100_000));
        let column = encode_checked(ColumnEncoding::FrameOfReference, Arc::new(array), &int64());
        assert_eq!(as_for(&column).deltas().byte_width(), 4);

        // 4-byte deltas but two distinct values: the dictionary is smaller.
        let array = Int64Array::from_iter_values((0..1000).map(|i| (i % 2) * 100_000_000));
        let column = encode_checked(ColumnEncoding::FrameOfReference, Arc::new(array), &int64());
        let dict = as_dict(&column);
        assert_eq!(dict.values.len(), 2);
        assert_eq!(dict.keys.byte_width(), 1);

        // Range beyond the frame of reference, few distinct values: the dictionary.
        let array = Int64Array::from_iter_values((0..1000).map(|i| [i64::MIN, 0, i64::MAX][i % 3]));
        let column = encode_checked(ColumnEncoding::FrameOfReference, Arc::new(array), &int64());
        assert_eq!(as_dict(&column).values.len(), 3);
    }

    #[test]
    fn test_encode_keeps_regular_column() {
        // Full range and all values distinct: no encoding is smaller.
        let array = Int32Array::from_iter_values((0..1000).map(|i| {
            if i % 2 == 0 {
                i32::MIN + i
            } else {
                i32::MAX - i
            }
        }));
        let data_type = DataType::Number(NumberDataType::Int32);
        let column = encode_checked(
            ColumnEncoding::FrameOfReference,
            Arc::new(array),
            &data_type,
        );
        assert!(matches!(column, Column::Number(_)));

        // Floats only take the dictionary.
        let array = Float64Array::from_iter_values((0..1000).map(|i| i as f64 / 7.0));
        let data_type = DataType::Number(NumberDataType::Float64);
        let column = encode_checked(ColumnEncoding::Dictionary, Arc::new(array), &data_type);
        assert!(matches!(column, Column::Number(_)));
    }

    #[test]
    fn test_encode_logical_types() {
        // Float with few distinct values.
        let array = Float64Array::from_iter_values((0..1000).map(|i| [0.5, 1.5, -2.0][i % 3]));
        let data_type = DataType::Number(NumberDataType::Float64);
        let column = encode_checked(ColumnEncoding::Dictionary, Arc::new(array), &data_type);
        assert_eq!(as_dict(&column).values.len(), 3);

        // Date: days since epoch over a few years.
        let array = Date32Array::from_iter_values((0..1000).map(|i| 8000 + i * 2));
        let column = encode_checked(
            ColumnEncoding::FrameOfReference,
            Arc::new(array),
            &DataType::Date,
        );
        assert_eq!(as_for(&column).deltas().byte_width(), 2);

        // Decimal(15, 2): raw (unscaled) values.
        let size = DecimalSize::new_unchecked(15, 2);
        let array = Decimal64Array::from_iter_values((0..1000).map(|i| 9_000_000 + i * 37))
            .with_precision_and_scale(15, 2)
            .unwrap();
        let data_type = DataType::Decimal(size);
        let column = encode_checked(
            ColumnEncoding::FrameOfReference,
            Arc::new(array),
            &data_type,
        );
        let for_column = as_for(&column);
        assert_eq!(for_column.base(), 9_000_000);
        assert_eq!(for_column.deltas().byte_width(), 2);
    }
}
