use common_datablocks::DataBlock;
use common_exception::Result;

trait Transform {
    fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;
}

