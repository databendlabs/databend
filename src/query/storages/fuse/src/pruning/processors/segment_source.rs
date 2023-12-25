use std::sync::Arc;

use databend_common_base::base::tokio::io::AsyncReadExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_pipeline_sources::AsyncSource;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::FuseLazyPartInfo;

/// 1. Get a `FuseLazyPartInfo` from `TableContext`
///
/// 2. Read segment withhout deserialization
struct SegmentSource {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
}

#[async_trait::async_trait]
impl AsyncSource for SegmentSource {
    const NAME: &'static str = "segment source";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let part = self.ctx.get_partition();
        if part.is_none() {
            return Ok(None);
        }
        let part = part.unwrap();
        let fuse_part = part.as_any().downcast_ref::<FuseLazyPartInfo>().unwrap();
        let mut reader = self.dal.reader(&fuse_part.segment_location.0).await?;
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer).await?;
        Ok(Some(DataBlock::empty_with_meta(Box::new(SegmentBytes {
            bytes: buffer,
        }))))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SegmentBytes {
    bytes: Vec<u8>,
}

#[typetag::serde(name = "segment_bytes")]
impl BlockMetaInfo for SegmentBytes {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("equals is unimplemented for SegmentBytes")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
