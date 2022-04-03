/// Partition status of data
pub enum PartitionState {
    Random,
    Broadcast,
    NotPartition,
    HashPartition,
    RangePartition,
}
