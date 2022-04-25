#[typetag::serde(tag = "type")]
pub trait ProcessInfo: Send + Sync {}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct FuseTableProcessInfo {
    pub id: usize,
    pub s3_elapse_ns: usize,
    pub read_data_block_rows: usize,
    pub read_data_block_bytes: usize,
    pub read_compressed_bytes: usize,
    pub deserialize_elapse_ns: usize,
}

#[typetag::serde(name = "fuse_source")]
impl ProcessInfo for FuseTableProcessInfo {}

impl FuseTableProcessInfo {
    pub fn create(
        id: usize,
        s3_elapse_ns: usize,
        read_data_block_rows: usize,
        read_data_block_bytes: usize,
        read_compressed_bytes: usize,
        deserialize_elapse_ns: usize,
    ) -> Box<dyn ProcessInfo> {
        Box::new(FuseTableProcessInfo {
            id,
            s3_elapse_ns,
            read_data_block_rows,
            read_data_block_bytes,
            read_compressed_bytes,
            deserialize_elapse_ns,
        })
    }
}
