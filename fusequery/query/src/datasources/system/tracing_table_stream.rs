// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::task::Poll;

use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::datatypes::SchemaRef;
use common_datablocks::DataBlock;
use common_datavalues::Int64Array;
use common_datavalues::Int8Array;
use common_exception::Result;
use futures::Stream;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct LogEntry {
    v: i64,
    name: String,
    msg: String,
    level: i8,
    hostname: String,
    pid: i64,
    time: String,
}

pub struct TracingTableStream {
    idx: usize,
    log_files: Vec<String>,
    schema: SchemaRef,
}

impl TracingTableStream {
    pub fn try_create(schema: SchemaRef, log_files: Vec<String>) -> Result<Self> {
        Ok(TracingTableStream {
            schema,
            log_files,
            idx: 0,
        })
    }

    pub fn try_get_one_block(&mut self) -> Result<Option<DataBlock>> {
        if self.idx >= self.log_files.len() {
            return Ok(None);
        }

        let mut version_col = vec![];
        let mut name_col = vec![];
        let mut msg_col = vec![];
        let mut level_col = vec![];
        let mut host_col = vec![];
        let mut pid_col = vec![];
        let mut time_col = vec![];

        let file = File::open(self.log_files[self.idx].clone())?;
        self.idx += 1;

        let reader = BufReader::new(file);
        for line in reader.lines() {
            let entry: LogEntry = serde_json::from_str(line.unwrap().as_str()).unwrap();
            version_col.push(entry.v);
            name_col.push(entry.name);
            msg_col.push(entry.msg);
            level_col.push(entry.level);
            host_col.push(entry.hostname);
            pid_col.push(entry.pid);
            time_col.push(entry.time);
        }

        let names: Vec<&str> = name_col.iter().map(|x| x.as_str()).collect();
        let msgs: Vec<&str> = msg_col.iter().map(|x| x.as_str()).collect();
        let hosts: Vec<&str> = host_col.iter().map(|x| x.as_str()).collect();
        let times: Vec<&str> = time_col.iter().map(|x| x.as_str()).collect();

        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Arc::new(Int64Array::from(version_col)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(msgs)),
            Arc::new(Int8Array::from(level_col)),
            Arc::new(StringArray::from(hosts)),
            Arc::new(Int64Array::from(pid_col)),
            Arc::new(StringArray::from(times)),
        ]);

        Ok(Some(block))
    }
}

impl Stream for TracingTableStream {
    type Item = Result<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let block = self.try_get_one_block()?;
        Poll::Ready(block.map(Ok))
    }
}
