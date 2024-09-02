// Copyright 2021 Datafuse Labs
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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_dot_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::ClusterOption;
use crate::ast::CreateOption;
use crate::ast::CreateTableSource;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::WarehouseOptions;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TargetLag {
    IntervalSecs(u64),
    Downstream,
}

impl Display for TargetLag {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TargetLag::IntervalSecs(secs) => {
                write!(f, "{} SECOND", secs)
            }
            TargetLag::Downstream => {
                write!(f, "DOWNSTREAM")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum RefreshMode {
    Auto,
    Full,
    Incremental,
}

impl Display for RefreshMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RefreshMode::Auto => {
                write!(f, "AUTO")
            }
            RefreshMode::Full => {
                write!(f, "FULL")
            }
            RefreshMode::Incremental => {
                write!(f, "INCREMENTAL")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum InitializeMode {
    OnCreate,
    OnSchedule,
}

impl Display for InitializeMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            InitializeMode::OnCreate => {
                write!(f, "ON_CREATE")
            }
            InitializeMode::OnSchedule => {
                write!(f, "ON_SCHEDULE")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateDynamicTableStmt {
    pub create_option: CreateOption,
    pub transient: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub source: Option<CreateTableSource>,
    pub cluster_by: Option<ClusterOption>,

    pub target_lag: TargetLag,
    pub warehouse_opts: WarehouseOptions,
    pub refresh_mode: RefreshMode,
    pub initialize: InitializeMode,

    pub table_options: BTreeMap<String, String>,
    pub as_query: Box<Query>,
}

impl Display for CreateDynamicTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if self.transient {
            write!(f, "TRANSIENT ")?;
        }
        write!(f, "DYNAMIC TABLE ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        if let Some(source) = &self.source {
            write!(f, " {source}")?;
        }

        if let Some(cluster_by) = &self.cluster_by {
            write!(f, " {cluster_by}")?;
        }

        write!(f, " TARGET_LAG = {}", self.target_lag)?;
        if self.warehouse_opts.warehouse.is_some() {
            write!(f, " {}", self.warehouse_opts)?;
        }
        write!(f, " REFRESH_MODE = {}", self.refresh_mode)?;
        write!(f, " INITIALIZE = {}", self.initialize)?;

        // Format table options
        if !self.table_options.is_empty() {
            write!(f, " ")?;
            write_space_separated_string_map(f, &self.table_options)?;
        }

        write!(f, " AS {}", self.as_query)?;
        Ok(())
    }
}
