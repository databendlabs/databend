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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

use databend_common_base::base::OrderedFloat;
use databend_common_exception::ErrorCode;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::converts::datavalues::from_scalar;
use databend_common_expression::converts::meta::IndexScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::F32;
use databend_common_frozen_api::FrozenAPI;
use databend_common_statistics::Datum;
use databend_common_vector::angular_distance;
use databend_common_vector::l1_distance;
use databend_common_vector::l2_distance;
use log::info;
use serde::de::Error;

use crate::meta::Location;
use crate::meta::RawBlockHLL;
use crate::meta::supported_stat_type;
use crate::meta::v0;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, FrozenAPI)]
pub struct ColumnStatistics {
    #[serde(
        serialize_with = "serialize_index_scalar",
        deserialize_with = "deserialize_index_scalar"
    )]
    min: Scalar,
    #[serde(
        serialize_with = "serialize_index_scalar",
        deserialize_with = "deserialize_index_scalar"
    )]
    max: Scalar,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

/// Column statistics bounds aligned with the current table schema.
///
/// Persisted decimal statistics may carry an older precision after a metadata-only precision
/// widening. The raw decimal value remains valid when the decimal kind and scale are unchanged,
/// so this view retags only the precision while leaving persisted metadata untouched.
#[derive(Clone, Debug)]
pub struct ColumnStatisticsView<'a> {
    min: Cow<'a, Scalar>,
    max: Cow<'a, Scalar>,
    null_count: u64,
}

impl ColumnStatisticsView<'_> {
    pub fn min(&self) -> &Scalar {
        &self.min
    }

    pub fn max(&self) -> &Scalar {
        &self.max
    }

    pub fn null_count(&self) -> u64 {
        self.null_count
    }

    pub fn datum_bounds(&self) -> (Option<Datum>, Option<Datum>) {
        (
            self.min.as_ref().clone().to_datum(),
            self.max.as_ref().clone().to_datum(),
        )
    }

    pub fn into_owned(self) -> ColumnStatisticsView<'static> {
        ColumnStatisticsView {
            min: Cow::Owned(self.min.into_owned()),
            max: Cow::Owned(self.max.into_owned()),
            null_count: self.null_count,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, FrozenAPI)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    #[serde(
        serialize_with = "serialize_index_scalar_vec",
        deserialize_with = "deserialize_index_scalar_vec"
    )]
    min: Vec<Scalar>,
    #[serde(
        serialize_with = "serialize_index_scalar_vec",
        deserialize_with = "deserialize_index_scalar_vec"
    )]
    max: Vec<Scalar>,
    pub level: i32,

    // Page pruning has been removed, but this field must remain in the persisted wire format so
    // binaries released before its removal can still deserialize newly written metadata.
    #[serde(
        default,
        serialize_with = "serialize_index_scalar_option_vec",
        deserialize_with = "deserialize_index_scalar_option_vec"
    )]
    pub pages: Option<Vec<Scalar>>,
}

/// Cluster statistics bounds aligned with the current cluster-key expression types.
///
/// As with [`ColumnStatisticsView`], Decimal precision may be retagged after a metadata-only
/// widening. Persisted bounds remain borrowed when no retagging is necessary.
#[derive(Clone, Debug)]
pub struct ClusterStatisticsView<'a> {
    min: Cow<'a, [Scalar]>,
    max: Cow<'a, [Scalar]>,
}

impl ClusterStatisticsView<'_> {
    pub fn min(&self) -> &[Scalar] {
        &self.min
    }

    pub fn max(&self) -> &[Scalar] {
        &self.max
    }
}

/// Exact values of the PARTITION BY expressions for a block or segment.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, FrozenAPI)]
pub struct PartitionStatistics {
    #[serde(
        serialize_with = "serialize_index_scalar_vec",
        deserialize_with = "deserialize_index_scalar_vec"
    )]
    pub values: Vec<Scalar>,
}

impl PartitionStatistics {
    pub fn new(values: Vec<Scalar>) -> Self {
        Self { values }
    }
}

pub fn validate_segment_partition_statistics<'a>(
    stats: impl IntoIterator<Item = Option<&'a PartitionStatistics>>,
) -> databend_common_exception::Result<Option<PartitionStatistics>> {
    // MODIFY COLUMN currently rejects every column referenced by PARTITION BY, so persisted and
    // newly generated values must have identical logical types. If that restriction is relaxed,
    // callers must first align values with the current partition-key types; treating a Decimal
    // precision-only difference as a different partition would turn a safe widening into a hard
    // metadata error.
    let mut partition = None;
    let mut has_unknown = false;
    for stats in stats {
        match (partition, stats) {
            (Some(expected), Some(actual)) if expected != actual => {
                return Err(ErrorCode::Internal(
                    "segment contains blocks from different partitions",
                ));
            }
            (None, Some(actual)) => partition = Some(actual),
            (_, None) => has_unknown = true,
            _ => {}
        }
    }
    if has_unknown {
        Ok(None)
    } else {
        Ok(partition.cloned())
    }
}

/// Spatial statistics for geometry columns.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default, FrozenAPI)]
pub struct SpatialStatistics {
    pub min_x: OrderedFloat<f64>,
    pub min_y: OrderedFloat<f64>,
    pub max_x: OrderedFloat<f64>,
    pub max_y: OrderedFloat<f64>,
    pub srid: i32,
    pub has_null: bool,
    #[serde(default)]
    pub has_empty_rect: bool,
    // Srid mixed or all rects are empty.
    #[serde(default)]
    pub is_valid: bool,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FrozenAPI,
)]
pub enum VectorDistanceType {
    L1,
    L2,
    Dot,
}

impl VectorDistanceType {
    pub fn from_index_option(distance_type: &str) -> Option<Self> {
        match distance_type.trim() {
            "cosine" | "dot" => Some(Self::Dot),
            "l1" => Some(Self::L1),
            "l2" => Some(Self::L2),
            _ => None,
        }
    }

    pub fn from_index_options<'a>(
        column_name: &str,
        distances: impl IntoIterator<Item = Option<&'a str>>,
    ) -> databend_common_exception::Result<Self> {
        let mut distance_types = Vec::new();
        for distance in distances {
            let Some(distance) = distance else {
                return Err(databend_common_exception::ErrorCode::InvalidClusterKeys(
                    format!(
                        "Vector cluster key `{column_name}` requires a vector index with distance option"
                    ),
                ));
            };

            for distance in distance.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let Some(distance_type) = Self::from_index_option(distance) else {
                    return Err(databend_common_exception::ErrorCode::InvalidClusterKeys(
                        format!(
                            "Vector cluster key `{column_name}` has unsupported vector index distance type `{distance}`"
                        ),
                    ));
                };
                if !distance_types.contains(&distance_type) {
                    distance_types.push(distance_type);
                }
            }
        }

        match distance_types.as_slice() {
            [distance_type] => Ok(*distance_type),
            [] => Err(databend_common_exception::ErrorCode::InvalidClusterKeys(
                format!(
                    "Vector cluster key `{column_name}` requires a vector index with distance option"
                ),
            )),
            _ => Err(databend_common_exception::ErrorCode::InvalidClusterKeys(
                format!(
                    "Vector cluster key `{column_name}` has multiple vector index distance types; use exactly one distance type for vector clustering"
                ),
            )),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Self::L1 => "l1".to_string(),
            Self::L2 => "l2".to_string(),
            Self::Dot => "dot".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, FrozenAPI)]
pub struct VectorColumnStatistics {
    pub centroid: Vec<F32>,
    pub radius: F32,
    pub row_count: u64,
}

impl VectorColumnStatistics {
    pub fn centroid_values(&self) -> Vec<f32> {
        self.centroid.iter().map(|value| value.0).collect()
    }

    pub fn spheres_overlap(
        &self,
        other: &VectorColumnStatistics,
        distance_type: VectorDistanceType,
    ) -> databend_common_exception::Result<bool> {
        let left_centroid = self.centroid_values();
        let right_centroid = other.centroid_values();
        let distance = match distance_type {
            VectorDistanceType::L1 => l1_distance(&left_centroid, &right_centroid)?,
            VectorDistanceType::L2 => l2_distance(&left_centroid, &right_centroid)?,
            VectorDistanceType::Dot => angular_distance(&left_centroid, &right_centroid)?,
        };

        Ok(distance <= self.radius.0 + other.radius.0)
    }

    pub fn distance_domain(
        &self,
        query: &[f32],
        distance_type: VectorDistanceType,
    ) -> databend_common_exception::Result<(f32, f32)> {
        let centroid = self.centroid_values();
        let distance = match distance_type {
            VectorDistanceType::L1 => l1_distance(query, &centroid)?,
            VectorDistanceType::L2 => l2_distance(query, &centroid)?,
            VectorDistanceType::Dot => angular_distance(query, &centroid)?,
        };
        let lower_bound = (distance - self.radius.0).max(0.0);
        if matches!(distance_type, VectorDistanceType::Dot) {
            let upper_bound = (distance + self.radius.0).min(std::f32::consts::PI);
            return Ok((1.0 - lower_bound.cos(), 1.0 - upper_bound.cos()));
        }

        Ok((lower_bound, distance + self.radius.0))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default, FrozenAPI)]
pub struct AdditionalStatsMeta {
    /// The size of the stats data in bytes.
    pub size: u64,
    /// The file location of the stats data.
    #[serde(default = "default_location")]
    pub location: Location,
    /// An optional HyperLogLog data structure.
    pub hll: Option<RawBlockHLL>,
    /// The count of the stats rows.
    #[serde(default)]
    pub row_count: u64,
    /// The count of the rows without statistics.
    #[serde(default)]
    pub unstats_rows: u64,
}

fn default_location() -> Location {
    ("".to_string(), 0)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default, FrozenAPI)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,
    pub perfect_block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub index_size: u64,
    pub bloom_index_size: Option<u64>,
    pub ngram_index_size: Option<u64>,
    pub inverted_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub spatial_index_size: Option<u64>,
    pub virtual_column_size: Option<u64>,

    #[serde(deserialize_with = "crate::meta::v2::statistics::deserialize_col_stats")]
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub virtual_col_stats: Option<HashMap<ColumnId, ColumnStatistics>>,
    pub spatial_stats: Option<HashMap<ColumnId, SpatialStatistics>>,
    pub cluster_stats: Option<ClusterStatistics>,
    #[serde(default)]
    pub partition_stats: Option<PartitionStatistics>,
    pub virtual_block_count: Option<u64>,

    pub additional_stats_meta: Option<AdditionalStatsMeta>,
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
        assert!(
            min.as_ref().infer_common_type(&max.as_ref()).is_some(),
            "must have same type, min: {min}, max: {max}",
        );

        Self {
            min,
            max,
            null_count,
            in_memory_size,
            distinct_of_values,
        }
    }

    /// Align persisted bounds with the current logical type.
    ///
    /// Returns `None` when the persisted values cannot safely represent the current type. Stats
    /// consumers must treat that case conservatively instead of comparing incompatible scalars.
    pub fn try_view(&self, data_type: &DataType) -> Option<ColumnStatisticsView<'_>> {
        let data_type = data_type.remove_nullable();
        let min = align_stat_scalar(&self.min, &data_type)?;
        let max = align_stat_scalar(&self.max, &data_type)?;
        min.as_ref()
            .as_ref()
            .infer_common_type(&max.as_ref().as_ref())?;
        Some(ColumnStatisticsView {
            min,
            max,
            null_count: self.null_count,
        })
    }

    pub fn try_view_with_table_type(
        &self,
        data_type: &TableDataType,
    ) -> Option<ColumnStatisticsView<'_>> {
        self.try_view(&DataType::from(data_type))
    }

    /// Raw persisted bound for serialization and statistics re-derivation only.
    /// Do not compare it; use [`Self::try_view`] with the current field type.
    pub(crate) fn raw_min(&self) -> &Scalar {
        &self.min
    }

    /// Raw persisted bound for serialization and statistics re-derivation only.
    /// Do not compare it; use [`Self::try_view`] with the current field type.
    pub(crate) fn raw_max(&self) -> &Scalar {
        &self.max
    }

    pub fn is_const(&self) -> bool {
        self.min == self.max
    }

    pub fn is_all_null(&self) -> bool {
        self.min.is_null() && self.max.is_null()
    }

    /// Reduce compatible column statistics into one range.
    ///
    /// Decimal bounds with the same kind and scale are aligned to the largest precision before
    /// comparison. Any genuinely incomparable input makes the statistics unusable.
    pub fn try_reduce(stats: &[&ColumnStatistics]) -> Option<ColumnStatistics> {
        if stats.is_empty() {
            return None;
        }
        let data_type = common_stats_type(stats)?;
        let mut min = None;
        let mut max = None;
        let mut null_count = 0;
        let mut in_memory_size = 0;
        let mut distinct_of_values = Some(0_u64);

        for stats in stats {
            let view = stats.try_view(&data_type)?;
            if !view.min().is_null() {
                match min.as_ref() {
                    None => min = Some(view.min().clone()),
                    Some(current) => match view.min().partial_cmp(current) {
                        Some(std::cmp::Ordering::Less) => min = Some(view.min().clone()),
                        Some(_) => {}
                        None => return None,
                    },
                }
            }
            if !view.max().is_null() {
                match max.as_ref() {
                    None => max = Some(view.max().clone()),
                    Some(current) => match view.max().partial_cmp(current) {
                        Some(std::cmp::Ordering::Greater) => max = Some(view.max().clone()),
                        Some(_) => {}
                        None => return None,
                    },
                }
            }
            null_count += stats.null_count;
            in_memory_size += stats.in_memory_size;
            distinct_of_values = match (distinct_of_values, stats.distinct_of_values) {
                (Some(total), Some(value)) => Some(total + value),
                _ => None,
            };
        }

        Some(ColumnStatistics::new(
            min.unwrap_or(Scalar::Null),
            max.unwrap_or(Scalar::Null),
            null_count,
            in_memory_size,
            distinct_of_values,
        ))
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

pub(crate) fn align_stat_scalar<'a>(
    scalar: &'a Scalar,
    data_type: &DataType,
) -> Option<Cow<'a, Scalar>> {
    if scalar.is_null() {
        return Some(Cow::Borrowed(scalar));
    }

    match (scalar, data_type) {
        (Scalar::Decimal(decimal), DataType::Decimal(target_size))
            if decimal.scale() == target_size.scale()
                // External Parquet statistics retain the physical Arrow variant, which may be
                // wider than the kind implied by the declared precision. Compare the persisted
                // and current logical widths here; the wider external container is preserved.
                && decimal.size().data_kind() == target_size.data_kind()
                && decimal.size().precision() <= target_size.precision() =>
        {
            if decimal.size() == *target_size {
                return Some(Cow::Borrowed(scalar));
            }
            let decimal = match decimal {
                DecimalScalar::Decimal64(value, _) => {
                    DecimalScalar::Decimal64(*value, *target_size)
                }
                DecimalScalar::Decimal128(value, _) => {
                    DecimalScalar::Decimal128(*value, *target_size)
                }
                DecimalScalar::Decimal256(value, _) => {
                    DecimalScalar::Decimal256(*value, *target_size)
                }
            };
            Some(Cow::Owned(Scalar::Decimal(decimal)))
        }
        _ if scalar.as_ref().infer_data_type() == *data_type => Some(Cow::Borrowed(scalar)),
        _ => None,
    }
}

fn common_stats_type(stats: &[&ColumnStatistics]) -> Option<DataType> {
    let mut target = None;
    for stats in stats {
        for scalar in [&stats.min, &stats.max] {
            if scalar.is_null() {
                continue;
            }
            let data_type = scalar.as_ref().infer_data_type();
            target = match target {
                None => Some(data_type),
                Some(DataType::Decimal(current)) => match data_type {
                    DataType::Decimal(candidate)
                        if current.data_kind() == candidate.data_kind()
                            && current.scale() == candidate.scale() =>
                    {
                        Some(DataType::Decimal(DecimalSize::new_unchecked(
                            current.precision().max(candidate.precision()),
                            current.scale(),
                        )))
                    }
                    _ => return None,
                },
                Some(current) if current == data_type => Some(current),
                Some(_) => return None,
            };
        }
    }
    Some(target.unwrap_or(DataType::Null))
}

impl ClusterStatistics {
    pub fn new(cluster_key_id: u32, min: Vec<Scalar>, max: Vec<Scalar>, level: i32) -> Self {
        Self {
            cluster_key_id,
            min,
            max,
            level,
            pages: None,
        }
    }

    /// Align persisted bounds with the current cluster-key expression types.
    ///
    /// Consumers must fail open when this returns `None`; comparing the persisted scalars
    /// directly can collapse incompatible Decimal sizes into `Ordering::Equal`.
    pub fn try_view(&self, data_types: &[DataType]) -> Option<ClusterStatisticsView<'_>> {
        if self.min.len() != data_types.len() || self.max.len() != data_types.len() {
            return None;
        }

        let min = align_stat_scalars(&self.min, data_types)?;
        let max = align_stat_scalars(&self.max, data_types)?;
        Some(ClusterStatisticsView { min, max })
    }

    /// Return an owned copy whose bounds are aligned with the current cluster-key types.
    pub fn normalized(&self, data_types: &[DataType]) -> Option<Self> {
        let view = self.try_view(data_types)?;
        Some(Self {
            cluster_key_id: self.cluster_key_id,
            min: view.min().to_vec(),
            max: view.max().to_vec(),
            level: self.level,
            pages: self.pages.clone(),
        })
    }

    /// Compare persisted cluster bounds without treating incompatible values as equal.
    pub fn try_cmp(&self, other: &Self) -> Option<Ordering> {
        let min = compare_statistics_scalar_slices(&self.min, &other.min)?;
        let max = compare_statistics_scalar_slices(&self.max, &other.max)?;
        if min != Ordering::Equal {
            return Some(min);
        }
        Some(max)
    }

    /// Raw persisted bounds for serialization and metadata conversion only.
    ///
    /// Do not compare these values. Use [`Self::try_view`] or a stats-specific comparison helper.
    pub(crate) fn raw_min(&self) -> &[Scalar] {
        &self.min
    }

    /// Raw persisted bounds for serialization and metadata conversion only.
    ///
    /// Do not compare these values. Use [`Self::try_view`] or a stats-specific comparison helper.
    pub(crate) fn raw_max(&self) -> &[Scalar] {
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

/// Compare scalars used by persisted statistics without changing global [`Scalar`] semantics.
///
/// Decimal precision is capacity rather than value identity. Physical variant and scale must
/// still match; all other incomparable values return `None`.
pub fn compare_statistics_scalars(left: &ScalarRef<'_>, right: &ScalarRef<'_>) -> Option<Ordering> {
    match (left, right) {
        (
            ScalarRef::Decimal(DecimalScalar::Decimal64(left, left_size)),
            ScalarRef::Decimal(DecimalScalar::Decimal64(right, right_size)),
        ) if left_size.scale() == right_size.scale() => left.partial_cmp(right),
        (
            ScalarRef::Decimal(DecimalScalar::Decimal128(left, left_size)),
            ScalarRef::Decimal(DecimalScalar::Decimal128(right, right_size)),
        ) if left_size.scale() == right_size.scale() => left.partial_cmp(right),
        (
            ScalarRef::Decimal(DecimalScalar::Decimal256(left, left_size)),
            ScalarRef::Decimal(DecimalScalar::Decimal256(right, right_size)),
        ) if left_size.scale() == right_size.scale() => left.partial_cmp(right),
        (ScalarRef::Decimal(_), ScalarRef::Decimal(_)) => None,
        _ => left.partial_cmp(right),
    }
}

pub fn compare_statistics_scalar_slices(left: &[Scalar], right: &[Scalar]) -> Option<Ordering> {
    if left.len() != right.len() {
        return None;
    }
    for (left, right) in left.iter().zip(right) {
        let ordering = compare_statistics_scalars(&left.as_ref(), &right.as_ref())?;
        if ordering != Ordering::Equal {
            return Some(ordering);
        }
    }
    Some(Ordering::Equal)
}

pub(crate) fn align_stat_scalars<'a>(
    scalars: &'a [Scalar],
    data_types: &[DataType],
) -> Option<Cow<'a, [Scalar]>> {
    let mut aligned = None;
    for (index, (scalar, data_type)) in scalars.iter().zip(data_types).enumerate() {
        let scalar = align_stat_scalar(scalar, &data_type.remove_nullable())?;
        if let Cow::Owned(scalar) = scalar {
            let values = aligned.get_or_insert_with(|| scalars.to_vec());
            values[index] = scalar;
        }
    }
    Some(match aligned {
        Some(values) => Cow::Owned(values),
        None => Cow::Borrowed(scalars),
    })
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
            bloom_index_size: None,
            ngram_index_size: None,
            inverted_index_size: None,
            vector_index_size: None,
            spatial_index_size: None,
            virtual_column_size: None,
            col_stats,
            virtual_col_stats: None,
            spatial_stats: None,
            cluster_stats: None,
            partition_stats: None,
            virtual_block_count: None,
            additional_stats_meta: None,
        }
    }

    pub fn additional_stats_loc(&self) -> Option<Location> {
        match &self.additional_stats_meta {
            Some(meta) if !meta.location.0.is_empty() => Some(meta.location.clone()),
            _ => None,
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
struct ColStatsVisitor {
    marker: PhantomData<fn() -> HashMap<ColumnId, ColumnStatistics>>,
}

impl ColStatsVisitor {
    fn new() -> Self {
        ColStatsVisitor {
            marker: PhantomData,
        }
    }
}

impl<'de> serde::de::Visitor<'de> for ColStatsVisitor {
    type Value = HashMap<ColumnId, ColumnStatistics>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where M: serde::de::MapAccess<'de> {
        let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some(key) = access.next_key::<ColumnId>()? {
            if let Ok(value) = access.next_value::<ColumnStatistics>() {
                if value.max.is_null() && value.min.is_null() {
                    // If scalar values of min and max are all NULL, they should be retained.
                    //
                    // This ensures that columns with only NULL values have their column statistics
                    // recorded, which is essential for pruning on these columns, and without this,
                    // column statistics like NDV (Number of Distinct Values) and null_count
                    // would be missing as well.
                    map.insert(key, value);
                } else {
                    let data_type = value.max.as_ref().infer_data_type();
                    if supported_stat_type(&data_type) {
                        map.insert(key, value);
                    } else {
                        info!(
                            "column of id {} is excluded from column statistics, unsupported data type {}",
                            key, data_type
                        );
                    }
                }
            }
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
    struct LegacyColumnStatistics {
        #[serde(
            serialize_with = "serialize_index_scalar",
            deserialize_with = "deserialize_index_scalar"
        )]
        min: Scalar,
        #[serde(
            serialize_with = "serialize_index_scalar",
            deserialize_with = "deserialize_index_scalar"
        )]
        max: Scalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    }

    #[derive(serde::Serialize)]
    struct ClusterStatisticsWithoutPages {
        cluster_key_id: u32,
        #[serde(serialize_with = "serialize_index_scalar_vec")]
        min: Vec<Scalar>,
        #[serde(serialize_with = "serialize_index_scalar_vec")]
        max: Vec<Scalar>,
        level: i32,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct ClusterStatisticsPublicLayout {
        cluster_key_id: u32,
        #[serde(
            serialize_with = "serialize_index_scalar_vec",
            deserialize_with = "deserialize_index_scalar_vec"
        )]
        min: Vec<Scalar>,
        #[serde(
            serialize_with = "serialize_index_scalar_vec",
            deserialize_with = "deserialize_index_scalar_vec"
        )]
        max: Vec<Scalar>,
        level: i32,
        #[serde(
            default,
            serialize_with = "serialize_index_scalar_option_vec",
            deserialize_with = "deserialize_index_scalar_option_vec"
        )]
        pages: Option<Vec<Scalar>>,
    }

    #[test]
    fn writes_pages_for_legacy_readers() {
        let stats = ClusterStatistics::new(
            7,
            vec![Scalar::Number(1_i64.into())],
            vec![Scalar::Number(9_i64.into())],
            2,
        );
        let bytes = rmp_serde::to_vec_named(&stats).unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(value.get("pages"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn reads_cluster_statistics_written_without_pages() {
        let stats = ClusterStatisticsWithoutPages {
            cluster_key_id: 7,
            min: vec![Scalar::Number(1_i64.into())],
            max: vec![Scalar::Number(9_i64.into())],
            level: 2,
        };
        let bytes = rmp_serde::to_vec_named(&stats).unwrap();
        let decoded: ClusterStatistics = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(decoded, ClusterStatistics::new(7, stats.min, stats.max, 2));
    }

    #[test]
    fn cluster_statistics_serialization_matches_public_field_layout() {
        let old = ClusterStatisticsPublicLayout {
            cluster_key_id: 7,
            min: vec![Scalar::Number(1_i64.into())],
            max: vec![Scalar::Number(9_i64.into())],
            level: 2,
            pages: None,
        };
        let new = ClusterStatistics::new(7, old.min.clone(), old.max.clone(), 2);

        let old_bytes = rmp_serde::to_vec_named(&old).unwrap();
        let new_bytes = rmp_serde::to_vec_named(&new).unwrap();
        assert_eq!(new_bytes, old_bytes);

        let decoded_new: ClusterStatistics = rmp_serde::from_slice(&old_bytes).unwrap();
        assert_eq!(decoded_new, new);
        let decoded_old: ClusterStatisticsPublicLayout = rmp_serde::from_slice(&new_bytes).unwrap();
        assert_eq!(decoded_old, old);
    }

    fn decimal64(value: i64, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal64(
            value,
            DecimalSize::new(precision, scale).unwrap(),
        ))
    }

    fn decimal128(value: i128, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal128(
            value,
            DecimalSize::new(precision, scale).unwrap(),
        ))
    }

    #[test]
    fn column_statistics_decimal_view_is_schema_aware() {
        let stats = ColumnStatistics::new(decimal64(1, 1, 0), decimal64(9, 1, 0), 2, 16, Some(9));
        let widened_size = DecimalSize::new(5, 0).unwrap();
        let view = stats
            .try_view(&DataType::Decimal(widened_size))
            .expect("same-kind Decimal precision widening is safe");
        assert_eq!(view.min(), &decimal64(1, 5, 0));
        assert_eq!(view.max(), &decimal64(9, 5, 0));
        assert_eq!(view.null_count(), 2);

        assert!(
            stats
                .try_view(&DataType::Decimal(DecimalSize::new(1, 0).unwrap()))
                .is_some()
        );
        assert!(
            stats
                .try_view(&DataType::Decimal(DecimalSize::new(1, 1).unwrap()))
                .is_none()
        );
        assert!(
            stats
                .try_view(&DataType::Decimal(DecimalSize::new(19, 0).unwrap()))
                .is_none()
        );

        let wider_stats =
            ColumnStatistics::new(decimal64(100, 10, 2), decimal64(900, 10, 2), 0, 16, None);
        assert!(
            wider_stats
                .try_view(&DataType::Decimal(DecimalSize::new(9, 2).unwrap()))
                .is_none()
        );
    }

    #[test]
    fn column_statistics_decimal_view_accepts_wider_external_variant() {
        let persisted_size = DecimalSize::new(9, 2).unwrap();
        let stats =
            ColumnStatistics::new(decimal128(100, 9, 2), decimal128(900, 9, 2), 0, 16, None);

        let view = stats
            .try_view(&DataType::Decimal(persisted_size))
            .expect("external Parquet stats may use a wider physical scalar variant");
        assert_eq!(view.min(), &decimal128(100, 9, 2));
        assert_eq!(view.max(), &decimal128(900, 9, 2));

        let widened_size = DecimalSize::new(15, 2).unwrap();
        let view = stats
            .try_view(&DataType::Decimal(widened_size))
            .expect("logical widening within the same kind remains compatible");
        assert_eq!(view.min(), &decimal128(100, 15, 2));
        assert_eq!(view.max(), &decimal128(900, 15, 2));

        assert!(
            stats
                .try_view(&DataType::Decimal(DecimalSize::new(19, 2).unwrap()))
                .is_none()
        );
    }

    #[test]
    fn cluster_statistics_decimal_view_is_schema_aware() {
        let stats = ClusterStatistics::new(
            3,
            vec![decimal64(100, 10, 2)],
            vec![decimal64(900, 10, 2)],
            1,
        );
        let widened_size = DecimalSize::new(15, 2).unwrap();
        let view = stats
            .try_view(&[DataType::Decimal(widened_size)])
            .expect("same-kind Decimal precision widening is safe");
        assert_eq!(view.min(), &[decimal64(100, 15, 2)]);
        assert_eq!(view.max(), &[decimal64(900, 15, 2)]);

        assert!(
            stats
                .try_view(&[DataType::Decimal(DecimalSize::new(9, 2).unwrap())])
                .is_none()
        );
        assert!(
            stats
                .try_view(&[DataType::Decimal(DecimalSize::new(15, 3).unwrap())])
                .is_none()
        );
        assert!(
            stats
                .try_view(&[DataType::Decimal(DecimalSize::new(19, 2).unwrap())])
                .is_none()
        );

        let widened = ClusterStatistics::new(
            3,
            vec![decimal64(200, 15, 2)],
            vec![decimal64(999, 15, 2)],
            1,
        );
        assert_eq!(stats.try_cmp(&widened), Some(Ordering::Less));
        let incompatible = ClusterStatistics::new(
            3,
            vec![decimal64(2000, 15, 3)],
            vec![decimal64(9990, 15, 3)],
            1,
        );
        assert_eq!(stats.try_cmp(&incompatible), None);
    }

    #[test]
    fn column_statistics_all_null_view_is_valid() {
        let stats = ColumnStatistics::new(Scalar::Null, Scalar::Null, 8, 0, Some(0));
        let view = stats
            .try_view(&DataType::Decimal(DecimalSize::new(15, 2).unwrap()))
            .unwrap();
        assert_eq!(view.min(), &Scalar::Null);
        assert_eq!(view.max(), &Scalar::Null);
        assert_eq!(view.null_count(), 8);
    }

    #[test]
    fn column_statistics_reduces_widened_decimal_in_either_order() {
        let old =
            ColumnStatistics::new(decimal64(100, 10, 2), decimal64(200, 10, 2), 1, 16, Some(2));
        let new =
            ColumnStatistics::new(decimal64(700, 15, 2), decimal64(800, 15, 2), 2, 16, Some(2));
        let expected_size = DecimalSize::new(15, 2).unwrap();

        for inputs in [[&old, &new], [&new, &old]] {
            let reduced = ColumnStatistics::try_reduce(&inputs).unwrap();
            let view = reduced.try_view(&DataType::Decimal(expected_size)).unwrap();
            assert_eq!(view.min(), &decimal64(100, 15, 2));
            assert_eq!(view.max(), &decimal64(800, 15, 2));
            assert_eq!(reduced.null_count, 3);
            assert_eq!(reduced.in_memory_size, 32);
            assert_eq!(reduced.distinct_of_values, Some(4));
        }

        let changed_scale =
            ColumnStatistics::new(decimal64(1, 10, 3), decimal64(2, 10, 3), 0, 16, None);
        assert!(ColumnStatistics::try_reduce(&[&old, &changed_scale]).is_none());

        let changed_kind = ColumnStatistics::new(
            Scalar::Decimal(DecimalScalar::Decimal128(
                1,
                DecimalSize::new(19, 2).unwrap(),
            )),
            Scalar::Decimal(DecimalScalar::Decimal128(
                2,
                DecimalSize::new(19, 2).unwrap(),
            )),
            0,
            32,
            None,
        );
        assert!(ColumnStatistics::try_reduce(&[&old, &changed_kind]).is_none());
    }

    #[test]
    fn column_statistics_serialization_matches_public_field_layout() {
        let legacy = LegacyColumnStatistics {
            min: decimal64(100, 10, 2),
            max: decimal64(900, 10, 2),
            null_count: 3,
            in_memory_size: 16,
            distinct_of_values: Some(9),
        };
        let current = ColumnStatistics::new(
            legacy.min.clone(),
            legacy.max.clone(),
            legacy.null_count,
            legacy.in_memory_size,
            legacy.distinct_of_values,
        );

        let legacy_bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        let current_bytes = rmp_serde::to_vec_named(&current).unwrap();
        assert_eq!(current_bytes, legacy_bytes);

        let decoded_current: ColumnStatistics = rmp_serde::from_slice(&legacy_bytes).unwrap();
        assert_eq!(decoded_current, current);
        let decoded_legacy: LegacyColumnStatistics = rmp_serde::from_slice(&current_bytes).unwrap();
        assert_eq!(decoded_legacy, legacy);
    }

    #[test]
    fn segment_partition_statistics_rejects_different_partitions() {
        let left = PartitionStatistics::new(vec![Scalar::Number(1_i64.into())]);
        let right = PartitionStatistics::new(vec![Scalar::Number(2_i64.into())]);

        assert_eq!(
            validate_segment_partition_statistics([Some(&left), Some(&left)]).unwrap(),
            Some(left.clone())
        );
        assert!(validate_segment_partition_statistics([Some(&left), Some(&right)]).is_err());
        assert_eq!(
            validate_segment_partition_statistics([None, Some(&left)]).unwrap(),
            None
        );
    }
}
