mod sync_source;
mod async_source;

pub use sync_source::SyncSource;
pub use async_source::AsyncSource;
pub use sync_source::SyncSourceProcessorWrap;
pub use async_source::ASyncSourceProcessorWrap;

mod source_example {
    use common_exception::Result;
    use common_datablocks::DataBlock;
    use crate::pipelines::new::processors::port::OutputPort;
    use crate::pipelines::new::processors::processor::ProcessorPtr;
    use crate::pipelines::new::processors::sources::{AsyncSource, ASyncSourceProcessorWrap, SyncSource, SyncSourceProcessorWrap};

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

    struct ExampleAsyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleAsyncSource {
        pub fn create(data_blocks: Vec<DataBlock>, outputs: Vec<OutputPort>) -> Result<ProcessorPtr> {
            ASyncSourceProcessorWrap::create(outputs, ExampleAsyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    #[async_trait::async_trait]
    impl AsyncSource for ExampleAsyncSource {
        async fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }
}

