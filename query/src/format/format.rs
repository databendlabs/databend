use std::any::Any;
use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_exception::Result;

pub trait InputState: Send {
    fn as_any(&mut self) -> &mut dyn Any;
}

pub trait InputFormat: Send {
    fn support_parallel(&self) -> bool {
        false
    }

    fn create_state(&self) -> Box<dyn InputState>;

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<DataBlock>;

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<usize>;
}




