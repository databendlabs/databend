mod async_source;
mod sync_source;
mod table_source;

pub use async_source::ASyncSourceProcessorWrap;
pub use async_source::AsyncSource;
pub use sync_source::SyncSource;
pub use sync_source::SyncSourceProcessorWrap;
pub use table_source::TableSource;

mod source_example {
    use std::sync::Arc;
    use common_datablocks::DataBlock;
    use common_exception::Result;

    use crate::pipelines::new::processors::port::OutputPort;
    use crate::pipelines::new::processors::processor::ProcessorPtr;
    use crate::pipelines::new::processors::sources::ASyncSourceProcessorWrap;
    use crate::pipelines::new::processors::sources::AsyncSource;
    use crate::pipelines::new::processors::sources::SyncSource;
    use crate::pipelines::new::processors::sources::SyncSourceProcessorWrap;

    struct ExampleSyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleSyncSource {
        pub fn create(
            data_blocks: Vec<DataBlock>,
            outputs: Vec<Arc<OutputPort>>,
        ) -> Result<ProcessorPtr> {
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
        pub fn create(
            data_blocks: Vec<DataBlock>,
            outputs: Vec<Arc<OutputPort>>,
        ) -> Result<ProcessorPtr> {
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
