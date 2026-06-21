use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::kvapi;
use databend_meta_client::types::InvalidArgument;

use crate::txn::for_update_target::ForUpdateTarget;

/// A key read for update and asserted to be absent.
pub struct AbsentForUpdate<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    pub(crate) target: ForUpdateTarget<'t, 'a, KV, K>,
}

impl<'t, 'a, KV, K> AbsentForUpdate<'t, 'a, KV, K>
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
