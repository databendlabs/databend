use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::ops::Read;
use crate::ops::ReadBuilder;

pub struct DataAccessor<'d, S> {
    s: Arc<S>,
    phantom: PhantomData<&'d ()>,
}

impl<'d, S> DataAccessor<'d, S> {
    pub fn new(s: S) -> DataAccessor<'d, S> {
        DataAccessor {
            s: Arc::new(s),
            phantom: PhantomData::default(),
        }
    }
}

impl<'d, S> DataAccessor<'d, S>
where S: Read<S>
{
    pub fn read(&self, path: &'d str) -> ReadBuilder<S> {
        ReadBuilder::new(self.s.clone(), path)
    }
}

// FIXME: maybe we need to mv StreamAccessor to parquet maybe.
pub struct StreamAccessor<'d, S> {
    s: Arc<S>,
    path: &'d str,

    total: usize,
    current: usize,
}

impl<S> futures::AsyncRead for StreamAccessor<'_, S>
where S: Read<S>
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        // TODO: implement Async Read for StreamAccessor
        unimplemented!()
    }
}
