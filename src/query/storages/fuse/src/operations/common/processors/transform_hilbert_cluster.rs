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

use std::cmp::Ordering;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::hilbert_index;
use databend_common_expression::types::BinaryType;
use databend_common_pipeline_transforms::AccumulatingTransform;

// Upper bound for per-dimension rank buckets. It keeps Hilbert coordinates
// compact while still preserving enough local order inside one recluster task.
const HILBERT_CLUSTER_MAX_BUCKETS: usize = 256;
// Extra bits refine ordering inside a bucket using the value's rank within that bucket.
const HILBERT_CLUSTER_SUB_BITS: usize = 4;

/// Accumulates one recluster task and appends a Hilbert proxy sort key column.
pub struct TransformHilbertCluster {
    dimension_offsets: Vec<usize>,
    dimension_values: Vec<HilbertDimensionValues>,
    pending_blocks: Vec<DataBlock>,
}

impl TransformHilbertCluster {
    /// Create a Hilbert clustering transform over 2 to 5 dimension columns.
    pub fn new(dimension_offsets: Vec<usize>) -> Self {
        debug_assert!((2..=5).contains(&dimension_offsets.len()));
        let dimension_values = std::iter::repeat_with(HilbertDimensionValues::default)
            .take(dimension_offsets.len())
            .collect();
        Self {
            dimension_offsets,
            dimension_values,
            pending_blocks: vec![],
        }
    }
}

impl AccumulatingTransform for TransformHilbertCluster {
    const NAME: &'static str = "TransformHilbertCluster";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        for (offset, values) in self
            .dimension_offsets
            .iter()
            .zip(self.dimension_values.iter_mut())
        {
            values.add_entry(data.get_by_offset(*offset));
        }
        self.pending_blocks.push(data);
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            self.pending_blocks.clear();
            self.dimension_values
                .iter_mut()
                .for_each(|values| values.values.clear());
            return Ok(vec![]);
        }

        if self.pending_blocks.is_empty() {
            return Ok(vec![]);
        }

        let row_count = self
            .pending_blocks
            .iter()
            .map(DataBlock::num_rows)
            .sum::<usize>();
        let bucket_count = hilbert_bucket_count(row_count);
        let bounds = self
            .dimension_values
            .iter_mut()
            .map(|values| values.take_bounds(bucket_count))
            .collect::<Vec<_>>();

        let mut blocks = std::mem::take(&mut self.pending_blocks);
        for block in &mut blocks {
            append_hilbert_sort_key_column(block, &self.dimension_offsets, &bounds, bucket_count)?;
        }
        Ok(blocks)
    }
}

#[derive(Default)]
struct HilbertDimensionValues {
    values: Vec<Scalar>,
}

struct HilbertDimensionBounds {
    bounds: Vec<Scalar>,
    sorted_values: Vec<Scalar>,
    bucket_ends: Vec<usize>,
}

impl HilbertDimensionValues {
    fn add_entry(&mut self, entry: &BlockEntry) {
        self.values.reserve(entry.len());
        for row in 0..entry.len() {
            let value = unsafe { entry.index_unchecked(row) };
            if !matches!(value, ScalarRef::Null) {
                self.values.push(value.to_owned());
            }
        }
    }

    fn take_bounds(&mut self, num_buckets: usize) -> HilbertDimensionBounds {
        debug_assert!(num_buckets >= 2);
        if self.values.len() <= 1 {
            let sorted_values = std::mem::take(&mut self.values);
            return HilbertDimensionBounds {
                bounds: vec![],
                bucket_ends: vec![sorted_values.len()],
                sorted_values,
            };
        }

        let mut values = std::mem::take(&mut self.values);
        values.sort_by(scalar_cmp);

        let mut bounds = Vec::with_capacity(num_buckets.saturating_sub(1));
        for bucket in 1..num_buckets {
            let index = bucket.saturating_mul(values.len()) / num_buckets;
            if index >= values.len() {
                break;
            }
            let bound = &values[index];
            if bounds
                .last()
                .is_none_or(|last| scalar_cmp(last, bound) != Ordering::Equal)
            {
                bounds.push(bound.clone());
            }
        }

        let mut cursor = 0usize;
        let mut bucket_ends = Vec::with_capacity(bounds.len() + 1);
        for bound in &bounds {
            while cursor < values.len()
                && scalar_ref_cmp(&values[cursor].as_ref(), &bound.as_ref()) != Ordering::Greater
            {
                cursor += 1;
            }
            bucket_ends.push(cursor);
        }
        bucket_ends.push(values.len());

        HilbertDimensionBounds {
            bounds,
            sorted_values: values,
            bucket_ends,
        }
    }
}

fn append_hilbert_sort_key_column(
    block: &mut DataBlock,
    dimension_offsets: &[usize],
    bounds: &[HilbertDimensionBounds],
    bucket_count: usize,
) -> Result<()> {
    let num_rows = block.num_rows();
    if num_rows == 0 {
        block.add_column(BinaryType::from_data(Vec::<Vec<u8>>::new()));
        return Ok(());
    }

    let mut sort_keys = Vec::with_capacity(num_rows);
    let coord_width = hilbert_coord_width_bytes(bucket_count, HILBERT_CLUSTER_SUB_BITS);
    let dimension_count = dimension_offsets.len();
    let dimension_entries = dimension_offsets
        .iter()
        .map(|offset| block.get_by_offset(*offset))
        .collect::<Vec<_>>();
    let mut coord_bytes = vec![0; dimension_count * coord_width];
    for row in 0..num_rows {
        for (dim, (entry, bounds)) in dimension_entries.iter().zip(bounds.iter()).enumerate() {
            let value = unsafe { entry.index_unchecked(row) };
            let coord = hilbert_coordinate(&value, bounds);
            let encoded = &mut coord_bytes[dim * coord_width..(dim + 1) * coord_width];
            let bytes = coord.to_be_bytes();
            encoded.copy_from_slice(&bytes[bytes.len() - coord_width..]);
        }

        let mut point: [&[u8]; 5] = [&[]; 5];
        for (dim, coord) in coord_bytes.chunks_exact(coord_width).enumerate() {
            point[dim] = coord;
        }
        sort_keys.push(hilbert_index(&point[..dimension_count], coord_width));
    }

    block.add_column(BinaryType::from_data(sort_keys));
    Ok(())
}

fn hilbert_bucket_count(row_count: usize) -> usize {
    row_count
        .max(2)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .min(HILBERT_CLUSTER_MAX_BUCKETS)
}

fn hilbert_coord_width_bytes(num_buckets: usize, sub_bits: usize) -> usize {
    debug_assert!(num_buckets >= 2);
    let coord_count = num_buckets
        .checked_shl(sub_bits as u32)
        .unwrap_or(usize::MAX);
    let bits = usize::BITS as usize - (coord_count.saturating_sub(1)).leading_zeros() as usize;
    bits.div_ceil(8)
}

fn hilbert_coordinate(value: &ScalarRef<'_>, bounds: &HilbertDimensionBounds) -> usize {
    let bucket_id = range_partition_id(value, &bounds.bounds);
    let sub_id = hilbert_sub_bucket_id(value, bounds, bucket_id);
    (bucket_id << HILBERT_CLUSTER_SUB_BITS) | sub_id
}

fn range_partition_id(value: &ScalarRef<'_>, bounds: &[Scalar]) -> usize {
    if matches!(value, ScalarRef::Null) {
        return bounds.len();
    }

    bounds.partition_point(|bound| scalar_ref_cmp(value, &bound.as_ref()) == Ordering::Greater)
}

fn hilbert_sub_bucket_id(
    value: &ScalarRef<'_>,
    bounds: &HilbertDimensionBounds,
    bucket_id: usize,
) -> usize {
    let null_sub_id = (1usize << HILBERT_CLUSTER_SUB_BITS) - 1;
    if matches!(value, ScalarRef::Null) {
        return null_sub_id;
    }
    let non_null_sub_buckets = null_sub_id.max(1);
    if bounds.sorted_values.len() <= 1 {
        return 0;
    }

    let bucket_start = bucket_id
        .checked_sub(1)
        .map(|idx| bounds.bucket_ends[idx])
        .unwrap_or(0);
    let bucket_end = bounds.bucket_ends[bucket_id];
    if bucket_end <= bucket_start + 1 {
        return 0;
    }

    let rank = bucket_start
        + bounds.sorted_values[bucket_start..bucket_end]
            .partition_point(|probe| scalar_ref_cmp(&probe.as_ref(), value) == Ordering::Less)
            .min(bucket_end - bucket_start - 1);
    let numerator = rank - bucket_start;
    let denominator = bucket_end - bucket_start;
    (numerator * non_null_sub_buckets / denominator).min(non_null_sub_buckets - 1)
}

fn scalar_cmp(left: &Scalar, right: &Scalar) -> Ordering {
    scalar_ref_cmp(&left.as_ref(), &right.as_ref())
}

fn scalar_ref_cmp(left: &ScalarRef<'_>, right: &ScalarRef<'_>) -> Ordering {
    if matches!(left, ScalarRef::Null) {
        return if matches!(right, ScalarRef::Null) {
            Ordering::Equal
        } else {
            Ordering::Greater
        };
    }

    if matches!(right, ScalarRef::Null) {
        return Ordering::Less;
    }

    left.partial_cmp(right).unwrap_or_else(|| {
        debug_assert!(
            false,
            "hilbert cluster dimension values must be comparable: {left:?}, {right:?}"
        );
        Ordering::Equal
    })
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    #[test]
    fn test_append_hilbert_sort_key_column() -> Result<()> {
        let mut block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3, 4]),
            Int32Type::from_data(vec![4, 3, 2, 1]),
        ]);
        let mut dim0 = HilbertDimensionValues::default();
        let mut dim1 = HilbertDimensionValues::default();
        dim0.add_entry(block.get_by_offset(0));
        dim1.add_entry(block.get_by_offset(1));
        let bounds = vec![
            dim0.take_bounds(hilbert_bucket_count(block.num_rows())),
            dim1.take_bounds(hilbert_bucket_count(block.num_rows())),
        ];

        let bucket_count = hilbert_bucket_count(block.num_rows());
        append_hilbert_sort_key_column(&mut block, &[0, 1], &bounds, bucket_count)?;

        assert_eq!(block.num_columns(), 3);
        let Column::Binary(keys) = block.get_by_offset(2).to_column() else {
            unreachable!("hilbert sort key should be binary");
        };
        assert_eq!(keys.len(), 4);
        assert!(keys.iter().all(|key| !key.is_empty()));
        Ok(())
    }

    #[test]
    fn test_finish_outputs_original_blocks_without_concat() -> Result<()> {
        let mut transform = TransformHilbertCluster::new(vec![0, 1]);
        let block1 = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2]),
            Int32Type::from_data(vec![6, 5]),
        ]);
        let block2 = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![3, 4, 5]),
            Int32Type::from_data(vec![4, 3, 2]),
        ]);

        assert!(transform.transform(block1)?.is_empty());
        assert_eq!(transform.dimension_values[0].values.len(), 2);
        assert_eq!(transform.dimension_values[1].values.len(), 2);
        assert!(transform.transform(block2)?.is_empty());
        assert_eq!(transform.dimension_values[0].values.len(), 5);
        assert_eq!(transform.dimension_values[1].values.len(), 5);
        let output = transform.on_finish(true)?;

        assert_eq!(output.len(), 2);
        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(output[1].num_rows(), 3);
        assert_eq!(output[0].num_columns(), 3);
        assert_eq!(output[1].num_columns(), 3);
        Ok(())
    }

    #[test]
    fn test_null_values_are_not_used_as_bounds() -> Result<()> {
        let block = DataBlock::new_from_columns(vec![Int32Type::from_opt_data(vec![
            Some(0),
            Some(1),
            None,
            Some(4),
            Some(8),
            None,
            Some(9),
        ])]);

        let mut dim = HilbertDimensionValues::default();
        dim.add_entry(block.get_by_offset(0));

        assert_eq!(dim.values.len(), 5);
        assert!(
            dim.values
                .iter()
                .all(|value| !matches!(value, Scalar::Null))
        );

        let bounds = dim.take_bounds(4);
        assert_eq!(bounds.bounds, vec![
            Scalar::Number(NumberScalar::Int32(1)),
            Scalar::Number(NumberScalar::Int32(4)),
            Scalar::Number(NumberScalar::Int32(8)),
        ]);
        assert!(
            bounds
                .bounds
                .iter()
                .all(|value| !matches!(value, Scalar::Null))
        );
        assert_eq!(
            range_partition_id(&ScalarRef::Number(NumberScalar::Int32(0)), &bounds.bounds),
            0
        );
        assert_eq!(
            range_partition_id(&ScalarRef::Number(NumberScalar::Int32(4)), &bounds.bounds),
            1
        );
        assert_eq!(
            range_partition_id(&ScalarRef::Number(NumberScalar::Int32(9)), &bounds.bounds),
            3
        );
        assert_eq!(
            range_partition_id(&ScalarRef::Null, &bounds.bounds),
            bounds.bounds.len()
        );
        Ok(())
    }

    #[test]
    fn test_hilbert_coord_width_bytes() {
        assert_eq!(hilbert_coord_width_bytes(2, 0), 1);
        assert_eq!(hilbert_coord_width_bytes(256, 0), 1);
        assert_eq!(hilbert_coord_width_bytes(256, 4), 2);
        assert_eq!(hilbert_coord_width_bytes(65536, 0), 2);
        assert_eq!(hilbert_coord_width_bytes(65537, 0), 3);
    }

    #[test]
    fn test_hilbert_bucket_sub_refinement_uses_rank_inside_bucket() {
        let block =
            DataBlock::new_from_columns(vec![Int32Type::from_data((0..16).collect::<Vec<_>>())]);
        let mut dim = HilbertDimensionValues::default();
        dim.add_entry(block.get_by_offset(0));
        let bounds = dim.take_bounds(2);

        let low = hilbert_coordinate(&ScalarRef::Number(NumberScalar::Int32(0)), &bounds);
        let mid = hilbert_coordinate(&ScalarRef::Number(NumberScalar::Int32(4)), &bounds);
        let high = hilbert_coordinate(&ScalarRef::Number(NumberScalar::Int32(7)), &bounds);
        let next_bucket = hilbert_coordinate(&ScalarRef::Number(NumberScalar::Int32(8)), &bounds);
        let null_coord = hilbert_coordinate(&ScalarRef::Null, &bounds);

        assert!(low < mid);
        assert!(mid < high);
        assert!(high < next_bucket);
        assert!(next_bucket < null_coord);
    }

    #[test]
    fn test_hilbert_bucket_count_is_bounded_and_adaptive() {
        assert_eq!(hilbert_bucket_count(0), 2);
        assert_eq!(hilbert_bucket_count(3), 4);
        assert_eq!(hilbert_bucket_count(257), HILBERT_CLUSTER_MAX_BUCKETS);
    }
}
