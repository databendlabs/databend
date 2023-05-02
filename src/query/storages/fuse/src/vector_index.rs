use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_cache::LoadParams;

use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn create_vector_index(&self) -> Result<()> {
        let snapshot_location = self.snapshot_loc().await?.ok_or(ErrorCode::Internal(
            "internal error, fuse table which navigated to given point has no snapshot location",
        ))?;
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: true,
        };
        let snapshot = reader.read(&params).await?;
        todo!()
    }
}
