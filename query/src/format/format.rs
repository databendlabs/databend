use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::Result;

pub trait InputState: Sized {}

pub trait InputFormat {
    fn support_parallel(&self) -> bool {
        false
    }

    fn create_state(&self) -> Box<dyn InputState>;

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<DataBlock>;

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize>;
}




