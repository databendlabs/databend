use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::kvapi;
use databend_meta_client::types::InvalidArgument;

use super::target::FetchedRecordTarget;

/// A fetched record asserted to be absent.
pub struct AbsentRecord<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    pub(crate) target: FetchedRecordTarget<'t, 'a, KV, K>,
}

impl<'t, 'a, KV, K> AbsentRecord<'t, 'a, KV, K>
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
