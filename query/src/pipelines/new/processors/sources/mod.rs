mod sync_source;
mod async_source;

pub use sync_source::SyncSource;
pub use sync_source::SyncSourceProcessorWrap;

mod source_example {
    use common_exception::Result;
    use common_datablocks::DataBlock;
    use crate::pipelines::new::processors::port::OutputPort;
    use crate::pipelines::new::processors::processor::ProcessorPtr;
    use crate::pipelines::new::processors::sources::{SyncSource, SyncSourceProcessorWrap};

    struct ExampleSyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleSyncSource {
        pub fn create(data_blocks: Vec<DataBlock>, outputs: Vec<OutputPort>) -> Result<ProcessorPtr> {
            SyncSourceProcessorWrap::create(outputs, ExampleSyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    impl SyncSource for ExampleSyncSource {
        fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }
}

