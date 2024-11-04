// Copyright 2023 Datafuse Labs.
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::connect;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_sinks::AsyncMpscSink;
use databend_common_pipeline_sinks::AsyncMpscSinker;

struct TestSink {
    count: Arc<AtomicUsize>,
}

impl TestSink {
    fn create(count: Arc<AtomicUsize>) -> Self {
        Self { count }
    }
}

#[async_trait]
impl AsyncMpscSink for TestSink {
    const NAME: &'static str = "TestSink";

    async fn on_start(&mut self) -> Result<()> {
        self.count.store(1, Ordering::SeqCst);
        Ok(())
    }

    async fn on_finish(&mut self) -> Result<()> {
        self.count.fetch_add(10, Ordering::SeqCst);
        Ok(())
    }

    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        self.count
            .fetch_add(data_block.num_rows(), Ordering::SeqCst);
        Ok(false)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_async_mpsc_sink() -> Result<()> {
    let input1 = InputPort::create();
    let input2 = InputPort::create();

    let count = Arc::new(AtomicUsize::new(0));

    let mut sink = AsyncMpscSinker::create(
        vec![input1.clone(), input2.clone()],
        TestSink::create(count.clone()),
    );

    let upstream_output1 = OutputPort::create();
    let upstream_output2 = OutputPort::create();

    unsafe {
        connect(&input1, &upstream_output1);
        connect(&input2, &upstream_output2);
    }

    upstream_output1.push_data(Ok(DataBlock::new(vec![], 1)));
    upstream_output2.push_data(Ok(DataBlock::new(vec![], 2)));

    // on start
    matches!(sink.event()?, Event::Async);
    sink.async_process().await?;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    // consume block1
    matches!(sink.event()?, Event::Async);
    sink.async_process().await?;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    // consume block2
    matches!(sink.event()?, Event::Async);
    sink.async_process().await?;
    assert_eq!(count.load(Ordering::SeqCst), 4);
    // on finish
    matches!(sink.event()?, Event::Async);
    sink.async_process().await?;
    assert_eq!(count.load(Ordering::SeqCst), 14);
    // finished
    matches!(sink.event()?, Event::Finished);

    Ok(())
}
