use std::io;

use map_api::mvcc;
use map_api::mvcc::ViewKey;
use map_api::mvcc::ViewValue;
use map_api::MapKey;
use seq_marked::SeqMarked;

use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::level::GetTable;
use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::map_api::MapKeyDecode;
use crate::leveled_store::map_api::MapKeyEncode;
use crate::leveled_store::value_convert::ValueConvert;

// TODO: test it
#[async_trait::async_trait]
impl<K> mvcc::ScopedSeqBoundedGet<K, K::V> for LeveledMap
where
    K: MapKey,
    K: ViewKey,
    K: MapKeyEncode + MapKeyDecode,
    K::V: ViewValue,
    SeqMarked<K::V>: ValueConvert<SeqMarked>,
    Level: GetTable<K, K::V>,
    Immutable: mvcc::ScopedSeqBoundedGet<K, K::V>,
{
    async fn get(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        let immutable = {
            let inner = self.data.lock().unwrap();
            let got = inner
                .writable
                .get_table()
                .get(key.clone(), snapshot_seq)
                .cloned();
            if !got.is_not_found() {
                return Ok(got);
            }

            inner.immutable.clone()
        };

        immutable.get(key, snapshot_seq).await
    }
}
