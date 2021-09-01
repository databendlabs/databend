// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::convert::TryInto;
use std::fs::File;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_runtime::tokio::task;
use common_streams::ParquetStream;
use common_streams::SendableDataBlockStream;
use crossbeam::channel::bounded;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use crate::catalogs::Table;
use crate::sessions::DatafuseQueryContextRef;

pub struct ParquetTable {
    db: String,
    name: String,
    schema: DataSchemaRef,
    file: String,
}

impl ParquetTable {
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        options: TableOptions,
    ) -> Result<Box<dyn Table>> {
        let file = options.get("location");
        return match file {
            Some(file) => {
                let table = ParquetTable {
                    db,
                    name,
                    schema,
                    file: file.trim_matches(|s| s == '\'' || s == '"').to_string(),
                };
                Ok(Box::new(table))
            }
            _ => Result::Err(ErrorCode::BadOption(
                "Parquet Engine must contains file location options".to_string(),
            )),
        };
    }
}

fn read_file(
    file: &str,
    tx: Sender<Option<Result<DataBlock>>>,
    projection: &[usize],
) -> Result<()> {
    let reader = File::open(file)?;
    let reader = read::RecordReader::try_new(
        reader,
        Some(projection.to_vec()),
        None,
        Arc::new(|_, _| true),
        None,
    )?;

    for maybe_batch in reader {
        match maybe_batch {
            Ok(batch) => {
                tx.send(Some(Ok(batch.try_into()?)))
                    .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
            }
            Err(e) => {
                let err_msg = format!("Error reading batch from {:?}: {}", file, e.to_string());

                tx.send(Some(Result::Err(ErrorCode::CannotReadFile(
                    err_msg.clone(),
                ))))
                .map_err(|send_error| ErrorCode::UnknownException(send_error.to_string()))?;

                return Result::Err(ErrorCode::CannotReadFile(err_msg));
            }
        }
    }

    Ok(())
}

#[async_trait::async_trait]
impl Table for ParquetTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "Parquet"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: DatafuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name().to_string(),
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: format!(
                "(Read from Parquet Engine table  {}.{})",
                self.db, self.name
            ),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        _ctx: DatafuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        type BlockSender = Sender<Option<Result<DataBlock>>>;
        type BlockReceiver = Receiver<Option<Result<DataBlock>>>;

        let (response_tx, response_rx): (BlockSender, BlockReceiver) = bounded(2);

        let file = self.file.clone();
        let projection: Vec<usize> = (0..self.schema.fields().len()).collect();
        task::spawn_blocking(move || {
            if let Err(e) = read_file(&file, response_tx, &projection) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream::try_create(response_rx)?))
    }
}
