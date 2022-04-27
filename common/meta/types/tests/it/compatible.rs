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

use common_meta_types::Cmd;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::DropDatabaseReq;
use common_meta_types::LogEntry;
use openraft::raft::Entry;
use openraft::raft::EntryPayload;

#[test]
#[ignore]
fn test_load_entry_compatibility() -> anyhow::Result<()> {
    let entries_before_20220413 = vec![
        r#"{"log_id":{"term":1,"index":2},"payload":{"Normal":{"txid":null,"cmd":{"AddNode":{"node_id":1,"node":{"name":"","endpoint":{"addr":"localhost","port":28103}}}}}}}"#,
        r#"{"log_id":{"term":1,"index":9},"payload":{"Normal":{"txid":null,"cmd":{"CreateDatabase":{"tenant":"test_tenant","name":"default","meta":{"engine":"","engine_options":{},"options":{},"created_on":"2022-02-22T01:51:06.980129Z"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":71},"payload":{"Normal":{"txid":null,"cmd":{"DropDatabase":{"tenant":"test_tenant","name":"db1"}}}}}"#,
        r#"{"log_id":{"term":1,"index":15},"payload":{"Normal":{"txid":null,"cmd":{"CreateTable":{"tenant":"test_tenant","db_name":"default","table_name":"tbl_01_0002","table_meta":{"schema":{"fields":[{"name":"a","default_expr":null,"data_type":{"type":"NullableType","inner":{"type":"Int32Type"},"name":"Nullable(Int32)"}}],"metadata":{}},"engine":"FUSE","engine_options":{},"options":{},"created_on":"2022-02-22T01:51:11.839689Z"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":18},"payload":{"Normal":{"txid":null,"cmd":{"DropTable":{"tenant":"test_tenant","db_name":"default","table_name":"tbl_01_0002"}}}}}"#,
        r#"{"log_id":{"term":1,"index":190},"payload":{"Normal":{"txid":null,"cmd":{"RenameTable":{"tenant":"test_tenant","db_name":"default","table_name":"05_0003_at_t0","new_db_name":"default","new_table_name":"05_0003_at_t1"}}}}}"#,
        r#"{"log_id":{"term":1,"index":16},"payload":{"Normal":{"txid":null,"cmd":{"UpsertTableOptions":{"table_id":1,"seq":{"Exact":1},"options":{"SNAPSHOT_LOC":"_ss/08b05df6d9264a419f402fc6e1c61b05"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":68},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"__fd_clusters/test_tenant/test_cluster/databend_query/XgXvK4QuPpih6EeHTHYeO6","seq":{"GE":1},"value":"AsIs","value_meta":{"expire_at":1645494749}}}}}}"#,
    ];

    for s in entries_before_20220413.iter() {
        let _ent: Entry<LogEntry> = serde_json::from_str(s)?;
    }

    // test value in old filed is converted
    {
        let ent: Entry<LogEntry> = serde_json::from_str(entries_before_20220413[1])?;
        match ent.payload {
            EntryPayload::Normal(LogEntry {
                cmd: Cmd::CreateDatabase(CreateDatabaseReq { db_name, .. }),
                ..
            }) => {
                assert_eq!("default", db_name);
            }
            _ => {
                unreachable!("");
            }
        }

        let ent: Entry<LogEntry> = serde_json::from_str(entries_before_20220413[2])?;
        match ent.payload {
            EntryPayload::Normal(LogEntry {
                cmd: Cmd::DropDatabase(DropDatabaseReq { db_name, .. }),
                ..
            }) => {
                assert_eq!("db1", db_name);
            }
            _ => {
                unreachable!("");
            }
        }
    }

    let entries_since_20220403 = vec![
        r#"{"log_id":{"term":1,"index":2},"payload":{"Normal":{"txid":null,"cmd":{"AddNode":{"node_id":1,"node":{"name":"1","endpoint":{"addr":"localhost","port":28103}}}}}}}"#,
        r#"{"log_id":{"term":1,"index":9},"payload":{"Normal":{"txid":null,"cmd":{"CreateDatabase":{"if_not_exists":true,"tenant":"test_tenant","db_name":"default","meta":{"engine":"","engine_options":{},"options":{},"created_on":"2022-04-15T05:24:34.324244Z"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":80},"payload":{"Normal":{"txid":null,"cmd":{"DropDatabase":{"if_exists":true,"tenant":"test_tenant","db_name":"db1"}}}}}"#,
        r#"{"log_id":{"term":1,"index":15},"payload":{"Normal":{"txid":null,"cmd":{"CreateTable":{"if_not_exists":false,"tenant":"test_tenant","db_name":"default","table_name":"tbl_01_0002","table_meta":{"schema":{"fields":[{"name":"a","default_expr":null,"data_type":{"type":"Int32Type"}}],"metadata":{}},"engine":"FUSE","engine_options":{},"options":{"database_id":"1"},"created_on":"2022-04-15T05:24:39.362029Z"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":18},"payload":{"Normal":{"txid":null,"cmd":{"DropTable":{"if_exists":false,"tenant":"test_tenant","db_name":"default","table_name":"tbl_01_0002"}}}}}"#,
        r#"{"log_id":{"term":1,"index":190},"payload":{"Normal":{"txid":null,"cmd":{"RenameTable":{"if_exists":false,"tenant":"test_tenant","db_name":"default","table_name":"05_0003_at_t0","new_db_name":"default","new_table_name":"05_0003_at_t1"}}}}}"#,
        r#"{"log_id":{"term":1,"index":210},"payload":{"Normal":{"txid":null,"cmd":{"UpsertTableOptions":{"table_id":65,"seq":{"Exact":83},"options":{"snapshot_location":"1/65/_ss/203f980ccce948cb9904bea1649f9e44_v1.json"}}}}}}"#,
        r#"{"log_id":{"term":1,"index":10},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"__fd_clusters/test_tenant/test_cluster/databend_query/x76ukR5f3LiW2WVeSRUjr3","seq":{"Exact":0},"value":{"Update":[123,34,105,100,34,58,34,120,55,54,117,107,82,53,102,51,76,105,87,50,87,86,101,83,82,85,106,114,51,34,44,34,99,112,117,95,110,117,109,115,34,58,49,48,44,34,118,101,114,115,105,111,110,34,58,48,44,34,102,108,105,103,104,116,95,97,100,100,114,101,115,115,34,58,34,48,46,48,46,48,46,48,58,57,48,57,49,34,125]},"value_meta":{"expire_at":1650000334}}}}}}"#,
    ];

    for s in entries_since_20220403.iter() {
        let _ent: Entry<LogEntry> = serde_json::from_str(s)?;
    }

    // test value in new filed is converted
    {
        let ent: Entry<LogEntry> = serde_json::from_str(entries_since_20220403[1])?;
        match ent.payload {
            EntryPayload::Normal(LogEntry {
                cmd: Cmd::CreateDatabase(CreateDatabaseReq { db_name, .. }),
                ..
            }) => {
                assert_eq!("default", db_name);
            }
            _ => {
                unreachable!("");
            }
        }

        let ent: Entry<LogEntry> = serde_json::from_str(entries_since_20220403[2])?;
        match ent.payload {
            EntryPayload::Normal(LogEntry {
                cmd: Cmd::DropDatabase(DropDatabaseReq { db_name, .. }),
                ..
            }) => {
                assert_eq!("db1", db_name);
            }
            _ => {
                unreachable!("");
            }
        }
    }

    Ok(())
}
