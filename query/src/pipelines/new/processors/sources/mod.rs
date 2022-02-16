// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod async_source;
mod sync_source;
mod sync_source_receiver;
mod table_source;

pub use async_source::AsyncSourcer;
pub use sync_source::SyncSource;
pub use sync_source::SyncSourcer;
pub use sync_source_receiver::SyncReceiverSource;
pub use table_source::TableSource;

#[allow(dead_code)]
mod source_example {
    use std::sync::Arc;

    use common_datablocks::DataBlock;
    use common_exception::Result;

    use crate::pipelines::new::processors::port::OutputPort;
    use crate::pipelines::new::processors::processor::ProcessorPtr;
    use crate::pipelines::new::processors::sources::async_source::AsyncSource;
    use crate::pipelines::new::processors::sources::sync_source::SyncSource;
    use crate::pipelines::new::processors::sources::AsyncSourcer;
    use crate::pipelines::new::processors::sources::SyncSourcer;

    struct ExampleSyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleSyncSource {
        pub fn create(
            data_blocks: Vec<DataBlock>,
            outputs: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            SyncSourcer::create(outputs, ExampleSyncSource {
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
            data_blocks: Vec<DataBlock>,
            output: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            AsyncSourcer::create(output, ExampleAsyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    #[async_trait::async_trait]
    impl AsyncSource for ExampleAsyncSource {
        const NAME: &'static str = "Async";

        async fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }
}
