// Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::tokio::sync::mpsc;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::processor_merge::MergeProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

// M inputs--> N outputs Mixed processor
struct MixedWorker {
    ctx: Arc<QueryContext>,
    n: usize,
    shared_num: AtomicUsize,
    started: AtomicBool,
    receivers: Vec<Option<mpsc::Receiver<Result<DataBlock>>>>,
    merger: MergeProcessor,
}

impl MixedWorker {
    pub fn start(&mut self) -> Result<()> {
        if self.started.load(Ordering::Relaxed) {
            return Ok(());
        }

        let inputs_len = self.merger.inputs().len();
        let outputs_len = self.n;

        let mut senders = Vec::with_capacity(outputs_len);
        for _i in 0..self.n {
            let (sender, receiver) = mpsc::channel::<Result<DataBlock>>(inputs_len);
            senders.push(sender);
            self.receivers.push(Some(receiver));
        }

        let mut stream = self.merger.merge()?;
        self.ctx.try_spawn(async move {
            let index = AtomicUsize::new(0);
            while let Some(item) = stream.next().await {
                let i = index.fetch_add(1, Ordering::Relaxed) % outputs_len;
                // TODO: USE try_reserve when the channel is blocking
                if let Err(error) = senders[i].send(item).await {
                    tracing::error!("Mixed processor cannot push data: {}", error);
                }
            }
        })?;

        self.started.store(true, Ordering::Relaxed);
        Ok(())
    }
}

pub struct MixedProcessor {
    worker: Arc<RwLock<MixedWorker>>,
    index: usize,
}

impl MixedProcessor {
    pub fn create(ctx: Arc<QueryContext>, n: usize) -> Self {
        let worker = MixedWorker {
            ctx: ctx.clone(),
            n,
            started: AtomicBool::new(false),
            shared_num: AtomicUsize::new(0),
            receivers: vec![],
            merger: MergeProcessor::create(ctx),
        };

        let index = worker.shared_num.fetch_add(1, Ordering::Relaxed);
        Self {
            worker: Arc::new(RwLock::new(worker)),
            index,
        }
    }

    pub fn share(&self) -> Result<Self> {
        let worker = self.worker.read();
        let index = worker.shared_num.fetch_add(1, Ordering::Relaxed);
        if index >= worker.n {
            return Err(ErrorCode::LogicalError("Mixed shared num overflow"));
        }

        Ok(Self {
            worker: self.worker.clone(),
            index,
        })
    }
}

#[async_trait::async_trait]
impl Processor for MixedProcessor {
    fn name(&self) -> &str {
        "MixedProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        let mut worker = self.worker.write();
        worker.merger.connect_to(input)
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        let worker = self.worker.read();
        worker.merger.inputs()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name = "mixed_processor_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let receiver = {
            let mut worker = self.worker.write();
            worker.start()?;
            worker.receivers[self.index].take()
        }
        .unwrap();

        Ok(Box::pin(ReceiverStream::new(receiver)))
    }
}
