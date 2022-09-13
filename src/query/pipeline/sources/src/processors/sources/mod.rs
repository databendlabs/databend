//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

pub mod async_source;
pub mod blocks_source;
pub mod deserializer;
pub mod empty_source;
pub mod file_splitter;
pub mod multi_file_splitter;
pub mod stream_source;
pub mod stream_source_v2;
pub mod sync_source;
pub mod sync_source_receiver;
mod one_block_source;

pub use async_source::AsyncSource;
pub use async_source::AsyncSourcer;
pub use async_source::*;
pub use blocks_source::BlocksSource;
pub use deserializer::Deserializer;
pub use empty_source::EmptySource;
pub use file_splitter::FileSplitter;
pub use file_splitter::FileSplitterState;
pub use multi_file_splitter::MultiFileSplitter;
pub use multi_file_splitter::OperatorInfo;
pub use stream_source::StreamSource;
pub use stream_source::StreamSourceNoSkipEmpty;
pub use stream_source_v2::StreamSourceV2;
pub use sync_source::SyncSource;
pub use sync_source::SyncSourcer;
pub use sync_source::*;
pub use sync_source_receiver::SyncReceiverSource;
pub use one_block_source::OneBlockSource;

#[allow(dead_code)]
mod source_example {
    use std::sync::Arc;

    use common_catalog::table_context::TableContext;
    use common_datablocks::DataBlock;
    use common_exception::Result;
    use common_pipeline_core::processors::port::OutputPort;
    use common_pipeline_core::processors::processor::ProcessorPtr;

    use crate::processors::sources::AsyncSource;
    use crate::processors::sources::AsyncSourcer;
    use crate::processors::sources::SyncSource;
    use crate::processors::sources::SyncSourcer;

    struct ExampleSyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleSyncSource {
        pub fn create(
            ctx: Arc<dyn TableContext>,
            data_blocks: Vec<DataBlock>,
            outputs: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            SyncSourcer::create(ctx, outputs, ExampleSyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    impl SyncSource for ExampleSyncSource {
        const NAME: &'static str = "Example";

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
            ctx: Arc<dyn TableContext>,
            data_blocks: Vec<DataBlock>,
            output: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            AsyncSourcer::create(ctx, output, ExampleAsyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    #[async_trait::async_trait]
    impl AsyncSource for ExampleAsyncSource {
        const NAME: &'static str = "Async";

        #[async_trait::unboxed_simple]
        async fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }
}
