use std::sync::Arc;
use common_exception::Result;

pub struct PublisherWorker {}

impl PublisherWorker {
    pub fn create() -> Arc<PublisherWorker> {
        // TODO:
    }

    pub fn listen(&self) -> Result<()> {}
}
