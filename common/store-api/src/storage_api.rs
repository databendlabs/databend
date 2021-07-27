// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_planners::Part;
use common_planners::PlanNode;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DataPartInfo {
    pub part: Part,
    pub stats: Statistics,
}

impl PartialEq for DataPartInfo {
    fn eq(&self, other: &Self) -> bool {
        self.part.name.eq(&other.part.name)
    }
}

impl Eq for DataPartInfo {}

impl Hash for DataPartInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.part.name.hash(state);
    }
}

pub type ReadPlanResult = Option<Vec<DataPartInfo>>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Database {
    pub database_id: u64,

    /// tables belong to this database.
    pub tables: HashMap<String, u64>,
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "database id: {}", self.database_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Table {
    pub table_id: u64,

    /// serialized schema
    pub schema: Vec<u8>,

    /// name of parts that belong to this table.
    pub parts: HashSet<DataPartInfo>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "table id: {}", self.table_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadAction {
    pub part: Part,
    pub push_down: PlanNode,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Summary {
    pub rows: usize,
    pub wire_bytes: usize,
    pub disk_bytes: usize,
}
impl Summary {
    pub(crate) fn increase(&mut self, rows: usize, wire_bytes: usize, disk_bytes: usize) {
        self.rows += rows;
        self.wire_bytes += wire_bytes;
        self.disk_bytes += disk_bytes;
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct PartitionInfo {
    pub rows: usize,
    pub cols: usize,
    pub wire_bytes: usize,
    pub disk_bytes: usize,
    pub location: String,
}

impl AppendResult {
    pub fn append_part(
        &mut self,
        location: &str,
        rows: usize,
        cols: usize,
        wire_bytes: usize,
        disk_bytes: usize,
    ) {
        let part = PartitionInfo {
            rows,
            cols,
            wire_bytes,
            disk_bytes,
            location: location.to_string(),
        };
        self.parts.push(part);
        self.summary.increase(rows, wire_bytes, disk_bytes);
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct AppendResult {
    pub summary: Summary,
    pub parts: Vec<PartitionInfo>,
    pub session_id: String,
    pub tx_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TruncateTableResult {}

// TODO A better name, we already have a SendableDataBlockStream
pub type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

#[async_trait::async_trait]
pub trait StorageApi {
    async fn read_plan(
        &mut self,
        db_name: String,
        tbl_name: String,
        scan_plan: &ScanPlan,
    ) -> common_exception::Result<ReadPlanResult>;

    /// Get partition.
    async fn read_partition(
        &mut self,
        schema: DataSchemaRef,
        read_action: &ReadAction,
    ) -> common_exception::Result<SendableDataBlockStream>;

    async fn append_data(
        &mut self,
        db_name: String,
        tbl_name: String,
        scheme_ref: DataSchemaRef,
        mut block_stream: BlockStream,
    ) -> common_exception::Result<AppendResult>;

    async fn truncate(
        &mut self,
        db: String,
        table: String,
    ) -> common_exception::Result<TruncateTableResult>;
}
