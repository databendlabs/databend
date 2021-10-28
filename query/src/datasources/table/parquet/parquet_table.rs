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
use common_base::tokio::task;
use common_context::DataContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::ReadDataSourcePlan;
use common_streams::ParquetStream;
use common_streams::SendableDataBlockStream;
use crossbeam::channel::bounded;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use crate::catalogs::Table;

pub struct ParquetTable {
    table_info: TableInfo,
    file: String,
}

impl ParquetTable {
    pub fn try_create(
        table_info: TableInfo,
        _data_ctx: Arc<dyn DataContext<u64>>,
    ) -> Result<Box<dyn Table>> {
        let options = table_info.options();
        let file = options.get("location").cloned();
        return match file {
            Some(file) => {
                let table = ParquetTable {
                    table_info,
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
    let reader = read::RecordReader::try_new(reader, Some(projection.to_vec()), None, None, None)?;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _io_ctx: Arc<TableIOContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        type BlockSender = Sender<Option<Result<DataBlock>>>;
        type BlockReceiver = Receiver<Option<Result<DataBlock>>>;

        let (response_tx, response_rx): (BlockSender, BlockReceiver) = bounded(2);

        let file = self.file.clone();
        let projection: Vec<usize> = plan.scan_fields().keys().cloned().collect::<Vec<_>>();

        task::spawn_blocking(move || {
            if let Err(e) = read_file(&file, response_tx, &projection) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream::try_create(response_rx)?))
    }
}
