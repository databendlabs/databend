// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_datablocks::DataBlock;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Streaming;

use crate::io::FS;
use crate::meta::TableMeta;
use crate::meta::TableSnapshot;

pub struct TableSpec {
    fs: Arc<dyn FS>,
}

const TBL_SPEC_V1: u8 = 0u8;

impl TableSpec {
    pub async fn create_table(&self, meta: &TableMeta) -> Result<TableSnapshot> {
        let io = &self.fs;

        // file location : /${db_name}/${table_name}/meta_v0.json
        let path = Path::new(&meta.db_name).join(&meta.tbl_name);
        let sequence = 0u64;
        let file_name = TableSpec::meta_file_name(sequence);

        let content = serde_json::to_string(&meta)?;
        io.put_if_absence(&path, &file_name, &content.as_bytes())
            .await?;

        Ok(TableSnapshot {
            spec_version: TBL_SPEC_V1,
            sequence,
            meta_uri: format!("/{}/{}", path.to_string_lossy(), file_name),
            table_name: meta.db_name.clone(),
            db_name: meta.db_name.clone(),
            table_uuid: meta.table_uuid,
        })
    }

    pub async fn insert(
        &self,
        tbl_meta: &TableMeta,
        req: Request<Streaming<FlightData>>,
    ) -> Result<()> {
        let req_meta = req.metadata();
        let (db_name, tbl, snapshot_id) = TableSpec::get_basic_info(req_meta)?;

        let mut stream = req.into_inner();
        self.write_data(tbl_meta, &mut stream).await?;
        Ok(())
    }

    async fn write_data(
        &self,
        tbl_meta: &TableMeta,
        stream: &mut Streaming<FlightData>,
    ) -> Result<()> {
        // build data file path
        let schema = tbl_meta.schema.clone();
        let data_file_name = uuid::Uuid::new_v4().to_simple().to_string() + ".parquet";
        let path = Path::new(&tbl_meta.db_name)
            .join(&tbl_meta.tbl_name)
            .join(data_file_name);

        // get an output stream for $db/$tbl/$uuid.parquet
        let mut writer = self.fs.writer(&path).await?;

        // convert stream-of-flight_data into stream-of_data_block
        let schema_ref = Arc::new(schema);
        let mut block_stream = stream.map(move |flight_data| {
            let batch = flight_data_to_arrow_batch(&flight_data?, schema_ref.clone(), &[])?;
            let r: Result<DataBlock, anyhow::Error> = batch.try_into();
            r
        });

        // write data info filesystem
        // buffering, partitioning, encoding ... are omitted by now
        while let Some(value) = block_stream.next().await {
            let block = value.context("malformed stream")?;
            writer
                .write_all(&block_to_parquet(block))
                .await
                .context("output stream failure")?;
        }

        // flush buffer
        writer.flush().await.context("flushing failure")?;

        // returns an snapshot (which will be committed to catalog)

        Ok(())
    }

    fn get_basic_info(req_meta: &MetadataMap) -> Result<(String, String, String)> {
        let db_name = req_meta
            .get("FUSE_DB")
            .context("missing database name")?
            .to_str()
            .context("invalid database name encoding")?
            .to_string();
        let tbl = req_meta
            .get("FUSE_TBL")
            .context("missing table name")?
            .to_str()
            .context("invalid table name encoding")?
            .to_string();
        let snapshot_id = req_meta
            .get("FUSE_TBL_SNAPSHOT")
            .context("missing snapshot id")?
            .to_str()
            .context("invalid snapshot id encoding")?
            .to_string();
        Ok((db_name, tbl, snapshot_id))
    }

    fn meta_file_name(ver: u64) -> String {
        format!("meta_v{}.json", ver)
    }
}

// should not go like this
fn block_to_parquet(_b: DataBlock) -> Vec<u8> {
    todo!()
}
