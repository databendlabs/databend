use std::sync::Arc;

use async_trait::async_trait;
use common_exception::Result;

use super::io::Reader;

#[async_trait]
pub trait Write<S: Send + Sync>: Send + Sync {
    async fn write(&self, r: Reader, args: &WriteBuilder<S>) -> Result<usize> {
        let (_, _) = (r, args);
        unimplemented!()
    }
}

pub struct WriteBuilder<'p, S> {
    s: Arc<S>,

    pub path: &'p str,
    pub size: usize,
}

impl<'p, S> WriteBuilder<'p, S> {
    pub fn new(s: Arc<S>, path: &'p str, size: usize) -> Self {
        Self { s, path, size }
    }
}

impl<'p, S: Write<S>> WriteBuilder<'p, S> {
    pub async fn run(&mut self, r: Reader) -> Result<usize> {
        self.s.write(r, self).await
    }
}
