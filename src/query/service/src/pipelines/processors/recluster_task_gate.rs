// Copyright 2021 Datafuse Labs
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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::NodeIndex;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use tokio::sync::watch;

/// Serialize task-local branches when a batch exceeds the worker's threads.
/// Only a completed branch may hand the node's resources to its successor.
pub(crate) struct ReclusterTaskGate {
    ready: watch::Receiver<bool>,
    done: watch::Sender<bool>,
    remaining: AtomicUsize,
}

impl ReclusterTaskGate {
    pub fn wrap(
        pipeline: &mut Pipeline,
        first_node: usize,
        previous: Option<&Arc<Self>>,
    ) -> Arc<Self> {
        let ready = previous.map_or_else(|| watch::channel(true).1, |gate| gate.done.subscribe());
        let gate = Arc::new(Self {
            ready,
            done: watch::channel(false).0,
            remaining: AtomicUsize::new(pipeline.graph.node_count() - first_node),
        });
        // Gate the whole branch, including read, sort and serialization. Gating
        // only its final output would still let queued sources consume resources.
        for node in pipeline.graph.node_weights_mut().skip(first_node) {
            node.proc = ProcessorPtr::create(Box::new(GatedProcessor {
                inner: node.proc.clone(),
                gate: gate.clone(),
                finished: false,
                pending: None,
            }));
        }
        gate
    }
}

struct GatedProcessor {
    inner: ProcessorPtr,
    gate: Arc<ReclusterTaskGate>,
    finished: bool,
    pending: Option<Event>,
}

// The inner pointer is private to this wrapper. The executor serializes its
// callbacks exactly as for an unwrapped processor, preserving ProcessorPtr safety.
#[async_trait::async_trait]
impl Processor for GatedProcessor {
    fn name(&self) -> String {
        unsafe { self.inner.name() }
    }

    fn as_any(&mut self) -> &mut dyn Any {
        unsafe { self.inner.as_any() }
    }

    fn set_id(&mut self, id: NodeIndex) {
        unsafe { self.inner.set_id(id) }
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if matches!(self.pending, Some(Event::Sync)) {
            return Ok(Event::Sync);
        }
        // Always deliver port events: event-only processors such as Resize must
        // not lose notifications while a branch waits. Defer only actual work.
        let event = unsafe { self.inner.event(cause)? };
        if matches!(event, Event::Sync | Event::Async) && !*self.gate.ready.borrow() {
            self.pending = Some(event);
            return Ok(Event::Async);
        }
        if matches!(event, Event::Finished) && !self.finished {
            self.finished = true;
            if self.gate.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.gate.done.send_replace(true);
            }
        }
        Ok(event)
    }

    fn un_reacted(&self, cause: EventCause, _id: usize) -> Result<()> {
        unsafe { self.inner.un_reacted(cause) }
    }

    fn process(&mut self) -> Result<()> {
        self.pending = None;
        unsafe { self.inner.process() }
    }

    async fn async_process(&mut self) -> Result<()> {
        if self.pending.is_some() {
            self.gate
                .ready
                .clone()
                .wait_for(|ready| *ready)
                .await
                .map_err(|_| ErrorCode::AbortedQuery("Recluster task predecessor closed"))?;
            if matches!(self.pending, Some(Event::Sync)) {
                return Ok(());
            }
            self.pending = None;
        }
        unsafe { self.inner.async_process() }.await
    }

    fn details_status(&self) -> Option<String> {
        unsafe { self.inner.details_status() }
    }
}
