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

use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::VariantType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_profile::OperatorAttribute;
use databend_common_profile::QueryProfileManager;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

// Encode an `OperatorAttribute` into jsonb::Value.
fn encode_operator_attribute(attr: &OperatorAttribute) -> jsonb::Value {
    match attr {
        OperatorAttribute::Join(join_attr) => (&serde_json::json! ({
            "join_type": join_attr.join_type,
            "equi_conditions": join_attr.equi_conditions,
            "non_equi_conditions": join_attr.non_equi_conditions,
        }))
            .into(),
        OperatorAttribute::Aggregate(agg_attr) => (&serde_json::json!({
            "group_keys": agg_attr.group_keys,
            "functions": agg_attr.functions,
        }))
            .into(),
        OperatorAttribute::AggregateExpand(expand_attr) => (&serde_json::json!({
            "group_keys": expand_attr.group_keys,
            "aggr_exprs": expand_attr.aggr_exprs,
        }))
            .into(),
        OperatorAttribute::Filter(filter_attr) => {
            (&serde_json::json!({ "predicate": filter_attr.predicate })).into()
        }
        OperatorAttribute::EvalScalar(scalar_attr) => {
            (&serde_json::json!({ "scalars": scalar_attr.scalars })).into()
        }
        OperatorAttribute::ProjectSet(project_attr) => {
            (&serde_json::json!({ "functions": project_attr.functions })).into()
        }
        OperatorAttribute::Limit(limit_attr) => (&serde_json::json!({
            "limit": limit_attr.limit,
            "offset": limit_attr.offset,
        }))
            .into(),
        OperatorAttribute::TableScan(scan_attr) => {
            (&serde_json::json!({ "qualified_name": scan_attr.qualified_name })).into()
        }
        OperatorAttribute::CteScan(cte_scan_attr) => {
            (&serde_json::json!({ "cte_idx": cte_scan_attr.cte_idx })).into()
        }
        OperatorAttribute::Sort(sort_attr) => {
            (&serde_json::json!({ "sort_keys": sort_attr.sort_keys })).into()
        }
        OperatorAttribute::Window(window_attr) => {
            (&serde_json::json!({ "functions": window_attr.functions })).into()
        }
        OperatorAttribute::Exchange(exchange_attr) => {
            (&serde_json::json!({ "exchange_mode": exchange_attr.exchange_mode })).into()
        }
        OperatorAttribute::Udf(udf_attr) => {
            (&serde_json::json!({ "scalars": udf_attr.scalars })).into()
        }
        OperatorAttribute::Empty => jsonb::Value::Null,
    }
}

pub struct QuerySummaryTable {
    table_info: TableInfo,
}

impl QuerySummaryTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("query_id", TableDataType::String),
            TableField::new("operator_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("operator_type", TableDataType::String),
            TableField::new(
                "operator_children",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new("operator_attribute", TableDataType::Variant),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_summary'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "query_summary".to_string(),
            meta: TableMeta {
                schema,
                engine: "QuerySummaryTable".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(Self { table_info })
    }
}

impl SyncSystemTable for QuerySummaryTable {
    const NAME: &'static str = "system.query_summary";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> databend_common_exception::Result<DataBlock> {
        let profile_mgr = QueryProfileManager::instance();
        let query_profs = profile_mgr.list_all();

        let mut query_ids: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut operator_ids: Vec<u32> = Vec::with_capacity(query_profs.len());
        let mut operator_types: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut operator_childrens: Vec<Vec<u32>> = Vec::with_capacity(query_profs.len());
        let mut operator_attributes: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());

        for prof in query_profs.iter() {
            for plan_prof in prof.operator_profiles.iter() {
                query_ids.push(prof.query_id.clone().into_bytes());
                operator_ids.push(plan_prof.id);
                operator_types.push(plan_prof.operator_type.to_string().into_bytes());
                operator_childrens.push(plan_prof.children.clone());

                let attribute_value = encode_operator_attribute(&plan_prof.attribute);
                operator_attributes.push(attribute_value.to_vec());
            }
        }

        let block = DataBlock::new_from_columns(vec![
            // query_id
            StringType::from_data(query_ids),
            // operator_id
            UInt32Type::from_data(operator_ids),
            // operator_type
            StringType::from_data(operator_types),
            // operator_children
            ArrayType::upcast_column(ArrayType::<UInt32Type>::column_from_iter(
                operator_childrens
                    .into_iter()
                    .map(|children| UInt32Type::column_from_iter(children.into_iter(), &[])),
                &[],
            )),
            // operator_attribute
            VariantType::from_data(operator_attributes),
        ]);

        Ok(block)
    }
}
