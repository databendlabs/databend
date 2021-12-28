use std::marker::PhantomData;
use std::sync::Arc;

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
