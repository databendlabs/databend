use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

/// Split partitioned metadata evenly into DataBlock chunks.
pub fn split_partitioned_meta_into_datablocks(
    bucket: isize,
    data: Vec<AggregateMeta>,
    outputs_len: usize,
) -> Vec<DataBlock> {
    if outputs_len == 0 {
        return vec![];
    }

    let total_len = data.len();
    let base_chunk_size = total_len / outputs_len;
    let remainder = total_len % outputs_len;

    let mut result = Vec::with_capacity(outputs_len);
    let mut data_iter = data.into_iter();

    for index in 0..outputs_len {
        let chunk_size = if index < remainder {
            base_chunk_size + 1
        } else {
            base_chunk_size
        };

        let chunk: Vec<AggregateMeta> = data_iter.by_ref().take(chunk_size).collect();
        result.push(DataBlock::empty_with_meta(
            AggregateMeta::create_partitioned(bucket, chunk),
        ));
    }

    result
}
