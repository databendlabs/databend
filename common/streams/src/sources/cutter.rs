use common_datablocks::DataBlock;
use common_exception::Result;

#[async_trait::async_trait]
pub trait Cutter {
    async fn get_chunk(&mut self) -> Result<Option<Vec<u8>>>;
}

trait NewSource {
    fn deserialize(&self, data: &[u8]) -> Result<DataBlock>;
}


