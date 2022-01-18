use std::cell::UnsafeCell;
use std::sync::Arc;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::node_index;
use petgraph::prelude::NodeIndex;

use common_exception::ErrorCode;
use common_exception::Result;
use crate::pipelines::new::unsafe_cell_wrap::UnSafeCellWrap;

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
    fn name(&self) -> &'static str;

    fn event(&mut self) -> Result<Event>;

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
    id: Arc<UnsafeCell<NodeIndex>>,
    inner: Arc<UnsafeCell<Box<dyn Processor>>>,
}

unsafe impl Send for ProcessorPtr {}

unsafe impl Sync for ProcessorPtr {}

impl ProcessorPtr {
    pub fn create(inner: Box<dyn Processor>) -> ProcessorPtr {
        ProcessorPtr {
            id: Arc::new(UnsafeCell::new(node_index(0))),
            inner: Arc::new(UnsafeCell::new(inner)),
        }
    }

    pub unsafe fn id(&self) -> NodeIndex {
        *self.id.get()
    }

    pub unsafe fn set_id(&self, id: NodeIndex) {
        *(&mut *self.id.get()) = id;
    }

    pub unsafe fn name(&self) -> &'static str {
        (&*self.inner.get()).name()
    }

    pub unsafe fn event(&self) -> Result<Event> {
        (&mut *self.inner.get()).event()
    }

    pub unsafe fn process(&self) -> Result<()> {
        (&mut *self.inner.get()).process()
    }

    pub unsafe fn async_process(&self) -> BoxFuture<'static, Result<()>> {
        (&mut *self.inner.get()).async_process().boxed()
    }
}

pub type Processors = Vec<ProcessorPtr>;
