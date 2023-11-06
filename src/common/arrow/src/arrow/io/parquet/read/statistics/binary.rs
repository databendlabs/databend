use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::Statistics as ParquetStatistics;

use crate::arrow::array::MutableArray;
use crate::arrow::array::MutableBinaryArray;
use crate::arrow::error::Result;
use crate::arrow::offset::Offset;

pub(super) fn push<O: Offset>(
    from: Option<&dyn ParquetStatistics>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
) -> Result<()> {
    let min = min
        .as_mut_any()
        .downcast_mut::<MutableBinaryArray<O>>()
        .unwrap();
    let max = max
        .as_mut_any()
        .downcast_mut::<MutableBinaryArray<O>>()
        .unwrap();
    let from = from.map(|s| s.as_any().downcast_ref::<BinaryStatistics>().unwrap());
    min.push(from.and_then(|s| s.min_value.as_ref()));
    max.push(from.and_then(|s| s.max_value.as_ref()));
    Ok(())
}
