// Copyright 2021 Datafuse Labs.
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

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
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
    schema: DataSchemaRef,
    file_idx: usize,
    log_files: Vec<String>,
    limit: usize,
    limit_offset: usize,
}

impl TracingTableStream {
    pub fn try_create(schema: DataSchemaRef, log_files: Vec<String>, limit: usize) -> Result<Self> {
        Ok(TracingTableStream {
            schema,
            log_files,
            file_idx: 0,
            limit,
            limit_offset: 0,
        })
    }

    pub fn try_get_one_block(&mut self) -> Result<Option<DataBlock>> {
        if self.file_idx >= self.log_files.len() {
            return Ok(None);
        }

        if self.limit_offset >= self.limit {
            return Ok(None);
        }

        let mut version_col = vec![];
        let mut name_col = vec![];
        let mut msg_col = vec![];
        let mut level_col = vec![];
        let mut host_col = vec![];
        let mut pid_col = vec![];
        let mut time_col = vec![];

        let file = File::open(self.log_files[self.file_idx].clone())?;
        self.file_idx += 1;

        let reader = BufReader::new(file);
        for line in reader.lines() {
            if self.limit_offset >= self.limit {
                break;
            }

            let entry: LogEntry = serde_json::from_str(line.unwrap().as_str())?;
            version_col.push(entry.v);
            name_col.push(entry.name);
            msg_col.push(entry.msg);
            level_col.push(entry.level);
            host_col.push(entry.hostname);
            pid_col.push(entry.pid);
            time_col.push(entry.time);
            self.limit_offset += 1;
        }

        let names: Vec<&[u8]> = name_col.iter().map(|x| x.as_bytes()).collect();
        let msgs: Vec<&[u8]> = msg_col.iter().map(|x| x.as_bytes()).collect();
        let hosts: Vec<&[u8]> = host_col.iter().map(|x| x.as_bytes()).collect();
        let times: Vec<&[u8]> = time_col.iter().map(|x| x.as_bytes()).collect();

        let block = DataBlock::create(self.schema.clone(), vec![
            Series::from_data(version_col),
            Series::from_data(names),
            Series::from_data(msgs),
            Series::from_data(level_col),
            Series::from_data(hosts),
            Series::from_data(pid_col),
            Series::from_data(times),
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
