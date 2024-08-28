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

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use databend_common_expression::converts::datavalues::from_scalar;
use databend_common_expression::converts::meta::IndexScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use serde::de::Error;

use crate::meta::v0;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    #[serde(
        serialize_with = "serialize_index_scalar",
        deserialize_with = "deserialize_index_scalar"
    )]
    pub min: Scalar,
    #[serde(
        serialize_with = "serialize_index_scalar",
        deserialize_with = "deserialize_index_scalar"
    )]
    pub max: Scalar,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    #[serde(
        serialize_with = "serialize_index_scalar_vec",
        deserialize_with = "deserialize_index_scalar_vec"
    )]
    pub min: Vec<Scalar>,
    #[serde(
        serialize_with = "serialize_index_scalar_vec",
        deserialize_with = "deserialize_index_scalar_vec"
    )]
    pub max: Vec<Scalar>,
    pub level: i32,

    #[serde(
        serialize_with = "serialize_index_scalar_option_vec",
        deserialize_with = "deserialize_index_scalar_option_vec"
    )]
    pub pages: Option<Vec<Scalar>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,
    pub perfect_block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub index_size: u64,

    #[serde(deserialize_with = "crate::meta::v2::statistics::deserialize_col_stats")]
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub cluster_stats: Option<ClusterStatistics>,
}

// conversions from old meta data
// ----------------------------------------------------------------
// ----------------------------------------------------------------
impl ColumnStatistics {
    pub fn new(
        min: Scalar,
        max: Scalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    ) -> Self {
        assert!(min.as_ref().infer_common_type(&max.as_ref()).is_some());

        Self {
            min,
            max,
            null_count,
            in_memory_size,
            distinct_of_values,
        }
    }

    pub fn min(&self) -> &Scalar {
        &self.min
    }

    pub fn max(&self) -> &Scalar {
        &self.max
    }

    pub fn from_v0(
        v0: &crate::meta::v0::statistics::ColumnStatistics,
        data_type: &TableDataType,
    ) -> Option<Self> {
        let data_type: DataType = data_type.into();

        if !matches!(
            data_type.remove_nullable(),
            DataType::Number(_)
                | DataType::Date
                | DataType::Timestamp
                | DataType::String
                | DataType::Decimal(_)
        ) {
            return None;
        }

        let min = from_scalar(&v0.min, &data_type);
        let max = from_scalar(&v0.max, &data_type);

        Some(Self {
            min,
            max,
            null_count: v0.null_count,
            in_memory_size: v0.in_memory_size,
            distinct_of_values: None,
        })
    }
}

impl ClusterStatistics {
    pub fn new(
        cluster_key_id: u32,
        min: Vec<Scalar>,
        max: Vec<Scalar>,
        level: i32,
        pages: Option<Vec<Scalar>>,
    ) -> Self {
        Self {
            cluster_key_id,
            min,
            max,
            level,
            pages,
        }
    }

    pub fn min(&self) -> &Vec<Scalar> {
        &self.min
    }

    pub fn max(&self) -> &Vec<Scalar> {
        &self.max
    }

    pub fn is_const(&self) -> bool {
        self.min.eq(&self.max)
    }

    pub fn from_v0(
        v0: crate::meta::v0::statistics::ClusterStatistics,
        data_type: &TableDataType,
    ) -> Option<Self> {
        let data_type: DataType = data_type.into();

        if !matches!(
            data_type.remove_nullable(),
            DataType::Number(_)
                | DataType::Date
                | DataType::Timestamp
                | DataType::String
                | DataType::Decimal(_)
        ) {
            return None;
        }

        let min = v0
            .min
            .into_iter()
            .map(|s| from_scalar(&s, &data_type))
            .collect();

        let max = v0
            .max
            .into_iter()
            .map(|s| from_scalar(&s, &data_type))
            .collect();

        Some(Self {
            cluster_key_id: v0.cluster_key_id,
            min,
            max,
            level: v0.level,
            pages: None,
        })
    }
}

impl Statistics {
    pub(crate) fn convert_column_stats(
        v0: &HashMap<ColumnId, v0::statistics::ColumnStatistics>,
        fields: &[TableField],
    ) -> HashMap<ColumnId, ColumnStatistics> {
        fields
            .iter()
            .filter_map(|f| {
                v0.get(&f.column_id).and_then(|v| {
                    ColumnStatistics::from_v0(v, f.data_type()).map(|v2| (f.column_id, v2))
                })
            })
            .collect()
    }

    pub fn from_v0(v0: crate::meta::v0::statistics::Statistics, fields: &[TableField]) -> Self {
        let col_stats = Self::convert_column_stats(&v0.col_stats, fields);
        Self {
            row_count: v0.row_count,
            block_count: v0.block_count,
            perfect_block_count: v0.perfect_block_count,
            uncompressed_byte_size: v0.uncompressed_byte_size,
            compressed_byte_size: v0.compressed_byte_size,
            index_size: v0.index_size,
            col_stats,
            cluster_stats: None,
        }
    }
}

/// Serializes a `Scalar` value by first converting it to `IndexScalar`.
///
/// This function indirectly uses `IndexScalar` for serialization because `IndexScalar`
/// ensures safe persistence to disk without being affected by version iterations.
/// Since `IndexScalar` is a subset of `Scalar`, serialization will fail if it attempts
/// to serialize a `Scalar` that is not supported by `IndexScalar`.
/// Callers should ensure that all `Scalar` values used for serialization are within
/// the supported subset of `IndexScalar`.
fn serialize_index_scalar<S>(scalar: &Scalar, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    match IndexScalar::try_from(scalar.clone()) {
        Ok(index_scalar) => serde::Serialize::serialize(&index_scalar, serializer),
        Err(e) => Err(serde::ser::Error::custom(format!(
            "Failed to convert scalar to IndexScalar: {:?}",
            e
        ))),
    }
}

/// Deserializes a value into a `Scalar` by first interpreting it as `IndexScalar`.
///
/// This function first deserializes the value into `IndexScalar` and then converts it
/// to `Scalar`.
fn deserialize_index_scalar<'de, D>(deserializer: D) -> Result<Scalar, D::Error>
where D: serde::Deserializer<'de> {
    let index_scalar = <IndexScalar as serde::Deserialize>::deserialize(deserializer)?;
    Scalar::try_from(index_scalar)
        .map_err(|e| D::Error::custom(format!("Failed to convert IndexScalar to Scalar: {:?}", e)))
}

/// Serializes a vector of `Scalar` values by first converting each to `IndexScalar`.
///
/// This function processes each `Scalar` in the vector, converting them to `IndexScalar`
/// for serialization. The use of `IndexScalar` is crucial for ensuring that the serialized
/// data is safe for persistence and unaffected by version iterations. Serialization will
/// fail if any `Scalar` in the vector is not a supported subset of `IndexScalar`.
/// Callers should verify that all `Scalar` values in the vector can be represented as
/// `IndexScalar`.
fn serialize_index_scalar_vec<S>(scalars: &[Scalar], serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    let index_scalars = scalars
        .iter()
        .map(|scalar| {
            IndexScalar::try_from(scalar.clone()).map_err(|e| {
                serde::ser::Error::custom(format!(
                    "Failed to convert Scalar to IndexScalar: {:?}",
                    e
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde::Serialize::serialize(&index_scalars, serializer)
}

/// Deserializes a value into a vector of `Scalar` by interpreting each element as `IndexScalar`.
///
/// This function deserializes a vector of `IndexScalar` values and then attempts to convert
/// each `IndexScalar` back into `Scalar`.
fn deserialize_index_scalar_vec<'de, D>(deserializer: D) -> Result<Vec<Scalar>, D::Error>
where D: serde::Deserializer<'de> {
    let index_scalars: Vec<IndexScalar> =
        <Vec<IndexScalar> as serde::Deserialize>::deserialize(deserializer)?;
    index_scalars
        .into_iter()
        .map(|index_scalar| {
            Scalar::try_from(index_scalar).map_err(|e| {
                D::Error::custom(format!("Failed to convert IndexScalar to Scalar: {:?}", e))
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn serialize_index_scalar_option_vec<S>(
    scalars: &Option<Vec<Scalar>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match scalars {
        Some(scalars) => serialize_index_scalar_vec(scalars, serializer),
        None => serializer.serialize_none(),
    }
}

fn deserialize_index_scalar_option_vec<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<Scalar>>, D::Error>
where D: serde::Deserializer<'de> {
    <Option<Vec<IndexScalar>> as serde::Deserialize>::deserialize(deserializer)?
        .map(|index_scalars| {
            index_scalars
                .into_iter()
                .map(|index_scalar| {
                    Scalar::try_from(index_scalar).map_err(|e| {
                        D::Error::custom(format!(
                            "Failed to convert IndexScalar to Scalar: {:?}",
                            e
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
}

/// Deserializes the `col_stats` field of the `BlockMeta` and `Statistics` struct.
///
/// This function is designed to handle legacy `ColumnStatistics` items that incorrectly
/// include unsupported `min` and `max` index types. In the new `IndexScalar` type, these
/// unsupported index types cannot be deserialized correctly.
///
/// To maintain forward compatibility and robustness, this function will skip any `col_stats`
/// item that fails to deserialize due to containing these unsupported index types.
/// This allows the rest of the outer struct, including `col_stats` items that do not
/// contain unsupported index types, to be deserialized successfully.
///
/// Note: This function is a workaround for a specific historical issue. If the data being
/// deserialized is known not to contain any unsupported index types in `ColumnStatistics`,
/// the standard deserialization process can be used instead.
pub fn deserialize_col_stats<'de, D>(
    deserializer: D,
) -> Result<HashMap<ColumnId, ColumnStatistics>, D::Error>
where D: serde::Deserializer<'de> {
    deserializer.deserialize_map(ColStatsVisitor::new())
}

struct ColStatsVisitor<K, V> {
    marker: PhantomData<fn() -> HashMap<K, V>>,
}

impl<K, V> ColStatsVisitor<K, V> {
    fn new() -> Self {
        ColStatsVisitor {
            marker: PhantomData,
        }
    }
}

impl<'de, K, V> serde::de::Visitor<'de> for ColStatsVisitor<K, V>
where
    K: serde::Deserialize<'de> + Hash + Eq,
    V: serde::Deserialize<'de>,
{
    type Value = HashMap<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where M: serde::de::MapAccess<'de> {
        let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some(key) = access.next_key()? {
            if let Ok(value) = access.next_value() {
                map.insert(key, value);
            }
        }

        Ok(map)
    }
}
