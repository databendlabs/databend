use std::cell::UnsafeCell;
use std::sync::Arc;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::NodeIndex;

use common_base::Runtime;
use common_exception::ErrorCode;
use common_exception::Result;
use crate::pipelines::new::pipeline::NewPipeline;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::UpdateList;

pub enum Event {
    NeedData,
    NeedConsume,
    Sync,
    Async,
    Finished,
}

// The design is inspired by ClickHouse processors
#[async_trait::async_trait]
pub trait Processor: Send {
    fn event(&mut self, ctx: &mut UpdateList) -> Result<Event>;

    // Synchronous work.
    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented process."))
    }

    // Asynchronous work.
    async fn async_process(&mut self) -> Result<()> {
        Err(ErrorCode::UnImplement("Unimplemented async_process."))
    }
}

#[derive(Clone)]
pub struct ProcessorPtr {
    inner: Arc<UnsafeCell<Box<dyn Processor>>>,
}

impl ProcessorPtr {
    pub fn create(inner: Box<dyn Processor>) -> ProcessorPtr {
        ProcessorPtr {
            inner: Arc::new(UnsafeCell::new(inner)),
        }
    }

    pub unsafe fn event(&self, ctx: &mut UpdateList) -> Result<Event> {
        (&mut *self.inner.get()).event(ctx)
    }

    pub unsafe fn process(&self) -> Result<()> {
        (&mut *self.inner.get()).process()
    }

    pub unsafe fn async_process(&self) -> BoxFuture<'static, Result<()>> {
        (&mut *self.inner.get()).async_process().boxed()
    }
}

pub type Processors = Vec<ProcessorPtr>;
