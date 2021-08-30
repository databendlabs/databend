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
//

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use std::sync::Arc;

    use anyhow::bail;
    use common_arrow::arrow::array::ArrayRef;
    use common_arrow::arrow::array::Int64Array;
    use common_arrow::arrow::io::ipc::write::common::IpcWriteOptions;
    use common_arrow::arrow::io::parquet::read;
    use common_arrow::arrow::record_batch::RecordBatch;
    use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
    use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
    use common_datablocks::DataBlock;
    use common_datavalues::prelude::*;
    use common_runtime::tokio;

    use crate::data_part::appender::*;
    use crate::localfs::LocalFS;

    #[test]
    fn test_in_memory_write() -> anyhow::Result<()> {
        let schema = Arc::new(DataSchema::new(vec![
            DataField::new("col_i", DataType::Int64, false),
            DataField::new("col_s", DataType::Utf8, false),
        ]));

        let col0 = Series::new(vec![0 as i64, 1, 2]);
        let col1 = Series::new(vec!["str1", "str2", "str3"]);
        let block = DataBlock::create_by_array(schema.clone(), vec![col0.clone(), col1.clone()]);

        let buffer = write_in_memory(block)?;
        let cursor = Cursor::new(buffer);
        let mut reader =
            read::RecordReader::try_new(cursor, None, None, Arc::new(|_, _| true), None)?;
        let arrow_schema = schema.to_arrow();

        if let Some(maybe_batch) = reader.next() {
            let batch = maybe_batch?;
            assert_eq!(batch.schema().as_ref(), &arrow_schema);
            assert_eq!(batch.column(0), &col0.get_array_ref());
            assert_eq!(batch.column(1), &col1.get_array_ref());
            Ok(())
        } else {
            bail!("empty record set?")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_append() -> anyhow::Result<()> {
        let col0: ArrayRef = Arc::new(Int64Array::from_values(vec![0, 1, 2]));
        let col1: ArrayRef = Arc::new(LargeUtf8Array::from_iter_values(
            vec!["str1", "str2", "str3"].iter(),
        ));

        let batch = RecordBatch::try_from_iter(vec![("col0", col0), ("col1", col1)])?;
        let schema = batch.schema();

        let p = tempfile::tempdir()?;
        let fs = LocalFS::try_create(p.path().to_str().unwrap().to_string())?;

        let appender = Appender::new(Arc::new(fs));

        let default_ipc_write_opt = IpcWriteOptions::default();
        let flight_schema = flight_data_from_arrow_schema(&schema, &default_ipc_write_opt);

        let req = futures::stream::iter(vec![
            flight_schema,
            flight_data_from_arrow_batch(&batch, &default_ipc_write_opt).1, // ignore dict
        ]);
        let r = appender
            .append_data("test_tbl".to_string(), Box::pin(req))
            .await;
        assert!(r.is_ok());
        Ok(())
    }
}
