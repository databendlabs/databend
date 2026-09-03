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

use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::Plan;

#[derive(Clone, Debug)]
pub struct CreateViewPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub column_names: Vec<String>,
    pub subquery: String,
    pub query_plan: Option<Box<Plan>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshLineageSelector {
    AllViews,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshLineagePlan {
    pub selector: RefreshLineageSelector,
    pub dry_run: bool,
}

impl RefreshLineagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("object_domain", DataType::String),
            DataField::new("catalog", DataType::String.wrap_nullable()),
            DataField::new("database", DataType::String.wrap_nullable()),
            DataField::new("object_name", DataType::String),
            DataField::new("status", DataType::String),
            DataField::new("edge_count", DataType::Number(NumberDataType::UInt64)),
            DataField::new("upsert_count", DataType::Number(NumberDataType::UInt64)),
            DataField::new("delete_count", DataType::Number(NumberDataType::UInt64)),
            DataField::new("error", DataType::String.wrap_nullable()),
        ])
    }
}

impl PartialEq for CreateViewPlan {
    fn eq(&self, other: &Self) -> bool {
        self.create_option == other.create_option
            && self.tenant == other.tenant
            && self.catalog == other.catalog
            && self.database == other.database
            && self.view_name == other.view_name
            && self.column_names == other.column_names
            && self.subquery == other.subquery
    }
}

impl Eq for CreateViewPlan {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterViewPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub column_names: Vec<String>,
    pub subquery: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropViewPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeViewPlan {
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub schema: DataSchemaRef,
}

impl DescribeViewPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
