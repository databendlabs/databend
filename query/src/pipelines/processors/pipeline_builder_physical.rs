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

use crate::pipelines::processors::{PipelineBuilder, Pipeline};
use crate::sql::planner::{PhysicalPlan, ColumnBinding};
use common_exception::Result;
use crate::sql::{TableScan, PhysicalFilter};
use crate::sessions::DatafuseQueryContextRef;
use crate::pipelines::transforms::{SourceTransform, FilterTransform};
use std::sync::Arc;
use common_datavalues::{DataSchema, DataType, DataField};
use std::str::FromStr;

pub struct PhysicalPipelineBuilder {
    ctx: DatafuseQueryContextRef,
}

impl PhysicalPipelineBuilder {
    pub fn create_schema(table_names: Vec<String>, column_names: Vec<String>, column_bindings: Vec<ColumnBinding>, data_types: Vec<DataType>) -> DataSchema {
        let mut field_names = vec![];
        for i in 0..table_names.len() {
            let (table_name, column_name, column_binding) = (table_names[i].as_str(), column_names[i].as_str(), &column_bindings[i]);
            field_names.push(Self::encode_field_name(table_name, column_name, column_binding));
        }

        let fields: Vec<DataField> = field_names.into_iter().zip(data_types).map(|(name, data_type)| {
            // TODO: support nullable
            DataField::new(name.as_str(), data_type, false)
        }).collect();
        DataSchema::new(fields)
    }

    // Encode (table_name, column_name, table_index, column_index)
    pub fn encode_field_name(table_name: &str, column_name: &str, column_binding: &ColumnBinding) -> String {
        format!("\"{}\"_\"{}\"_{}_{}", table_name, column_name, column_binding.table_index, column_binding.column_index)
    }

    // Decode field name to (table_name, column_name, column_binding)
    pub fn decode_field_name(field_name: String) -> Result<(String, String, ColumnBinding)> {
        todo!()
    }

    fn build_impl(&self, physical_plan: &PhysicalPlan) -> Result<Pipeline> { todo!() }


    // fn build_filter(&self, filter: &PhysicalFilter) -> Result<Pipeline> {
    //     let mut pipeline = self.build_impl(&filter.child)?;
    //     let schema = Self::create_schema();
    //     pipeline.add_simple_transform(|| {
    //         Ok(Box::new(FilterTransform::try_create(
    //             node.schema(),
    //             node.predicate.clone(),
    //             false,
    //         )?))
    //     })?;
    //     Ok(pipeline)
    // }

    fn build_table_scan(&self, table_scan: &TableScan) -> Result<Pipeline> {
        // let table = self.ctx.get_table(table_scan.get_plan.db_name.as_str(), table_scan.get_plan.table_name.as_str())?;
        // let read_plan = table.datasource().read_plan()?;
        //
        // self.ctx.try_set_partitions(read_plan.parts)?;
        // let mut pipeline = Pipeline::create(self.ctx.clone());
        // let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        // let max_threads = std::cmp::min(max_threads, read_plan.parts.len());
        // let workers = std::cmp::max(max_threads, 1);
        //
        // for _i in 0..workers {
        //     let source = SourceTransform::try_create(self.ctx.clone(), read_plan.clone())?;
        //     pipeline.add_source(Arc::new(source))?;
        // }
        // Ok(pipeline)
        todo!()
    }
}