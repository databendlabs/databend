use std::sync::Arc;

use async_trait::async_trait;
use common_exception::Result;

use super::io::Reader;

#[async_trait]
pub trait Read<S: Send + Sync>: Send + Sync {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let _ = args;
        unimplemented!()
    }
}

pub struct ReadBuilder<'p, S> {
    s: Arc<S>,

    pub path: &'p str,
    pub offset: Option<usize>,
    pub size: Option<usize>,
}

impl<'p, S> ReadBuilder<'p, S> {
    pub fn new(s: Arc<S>, path: &'p str) -> Self {
        Self {
            s,
            path,
            offset: None,
            size: None,
        }
    }

    pub fn offset(&mut self, offset: usize) -> &mut Self {
        self.offset = Some(offset);

        self
    }

    pub fn size(&mut self, size: usize) -> &mut Self {
        self.size = Some(size);

        self
    }
}

impl<'p, S: Read<S>> ReadBuilder<'p, S> {
    pub async fn run(&mut self) -> Result<Reader> {
        self.s.read(self).await
    }
}
