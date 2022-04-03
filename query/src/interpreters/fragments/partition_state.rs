/// Partition status of data
#[derive(PartialEq)]
pub enum PartitionState {
    Random,
    Broadcast,
    NotPartition,
    HashPartition,
    RangePartition,
}
