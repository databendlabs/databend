use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;

pub struct BlockPruningResult {
    pub block_metas: Vec<(Option<BlockMetaIndex>, Arc<BlockMeta>)>,
}

impl BlockPruningResult {
    pub fn create(block_metas: Vec<(Option<BlockMetaIndex>, Arc<BlockMeta>)>) -> BlockMetaInfoPtr {
        Box::new(BlockPruningResult { block_metas })
    }
}

#[typetag::serde(name = "block_pruning_result")]
impl BlockMetaInfo for BlockPruningResult {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals BlockPruningResult")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone BlockPruningResult")
    }
}

impl Debug for BlockPruningResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockPruningResult")
            .field("block_metas", &self.block_metas)
            .finish()
    }
}

impl serde::Serialize for BlockPruningResult {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize BlockPruningResult")
    }
}

impl<'de> serde::Deserialize<'de> for BlockPruningResult {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize BlockPruningResult")
    }
}
