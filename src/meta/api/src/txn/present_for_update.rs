use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::kvapi;
use databend_meta_client::types::InvalidArgument;
use databend_meta_client::types::SeqV;

use crate::txn::for_update_target::ForUpdateTarget;

/// A key read for update and asserted to be present.
pub struct PresentForUpdate<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    pub(crate) target: ForUpdateTarget<'t, 'a, KV, K>,
    pub(crate) seq_v: SeqV<K::ValueType>,
}

impl<'t, 'a, KV, K> PresentForUpdate<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
{
    /// The version read.
    pub fn seq(&self) -> u64 {
        self.seq_v.seq
    }

    /// The full record read: seq, meta, and value.
    pub fn seq_v(&self) -> &SeqV<K::ValueType> {
        &self.seq_v
    }

    /// The value read.
    pub fn value(&self) -> &K::ValueType {
        &self.seq_v.data
    }

    /// Consume the handle, yielding the value read.
    pub fn into_value(self) -> K::ValueType {
        self.seq_v.data
    }

    /// Stage a delete of the read key.
    pub fn delete(self) {
        self.target.delete();
    }
}

impl<'t, 'a, KV, K> PresentForUpdate<'t, 'a, KV, K>
where
    KV: KVApi + ?Sized,
    KV::Error: From<InvalidArgument>,
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    /// Stage a put to the read key.
    pub fn put(self, value: &K::ValueType) -> Result<(), KV::Error> {
        self.target.put(value)
    }
}
