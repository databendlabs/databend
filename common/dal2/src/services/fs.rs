use std::io::SeekFrom;

use async_compat::CompatExt;
use async_trait::async_trait;
use common_exception::Result;
use futures;
use tokio;
use tokio::io::AsyncSeek;
use tokio::io::AsyncSeekExt;

use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;

#[derive(Default)]
pub struct Builder {}

impl Builder {
    pub fn finish(self) -> Backend {
        Backend {}
    }
}

pub struct Backend {}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl<S: Send + Sync> Read<S> for Backend {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let mut f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&args.path)
            .await
            .unwrap();

        if args.offset.is_some() {
            f.seek(SeekFrom::Start(args.offset.unwrap() as u64)).await?;
        }

        if args.size.is_some() {
            f.set_len(args.size.unwrap() as u64).await?;
        }

        Ok(Box::new(f.compat()))
    }
}
