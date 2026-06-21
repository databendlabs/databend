use databend_common_proto_conv::FromToProto;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::kvapi::kvapi;
use databend_meta_client::types::InvalidArgument;

use crate::MetaTxn;

pub(crate) struct FetchedRecordTarget<'t, 'a, KV: ?Sized, K: kvapi::Key> {
    pub(crate) txn: &'t MetaTxn<'a, KV>,
    pub(crate) key: K,
}

impl<'t, 'a, KV, K> FetchedRecordTarget<'t, 'a, KV, K>
where
    KV: ?Sized,
    K: kvapi::Key,
{
    pub(crate) fn delete(self) {
        self.txn.delete(&self.key);
    }
}

impl<'t, 'a, KV, K> FetchedRecordTarget<'t, 'a, KV, K>
where
    KV: KVApi + ?Sized,
    KV::Error: From<InvalidArgument>,
    K: kvapi::Key,
    K::ValueType: FromToProto + 'static,
{
    pub(crate) fn put(self, value: &K::ValueType) -> Result<(), KV::Error> {
        self.txn.put(&self.key, value)
    }
}
