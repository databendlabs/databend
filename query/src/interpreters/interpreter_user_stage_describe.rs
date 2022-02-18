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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::DescribeUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

pub struct DescribeUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescribeUserStagePlan,
}

impl DescribeUserStageInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: DescribeUserStagePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(DescribeUserStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DescribeUserStageInterpreter {
    fn name(&self) -> &str {
        "DescribeUserStageInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let schema = self.plan.schema();
        let default_stage = UserStageInfo::default();

        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        let stage = user_mgr.get_stage(&tenant, self.plan.name.as_str()).await?;

        let mut parent_properties: Vec<&str> = vec![];
        let mut properties: Vec<&str> = vec![];
        let mut property_types: Vec<&str> = vec![];
        let mut property_values: Vec<String> = vec![];
        let mut property_defaults: Vec<String> = vec![];

        let params = &stage.stage_params;

        // url
        parent_properties.push("stage_params");
        properties.push("url");
        property_types.push("String");
        property_values.push(params.url.clone());
        property_defaults.push(default_stage.stage_params.url.clone());

        // credentials
        parent_properties.push("credentials");
        properties.push("access_key_id");
        property_types.push("String");
        property_values.push(params.credentials.access_key_id.clone());
        property_defaults.push(default_stage.stage_params.credentials.access_key_id.clone());

        parent_properties.push("credentials");
        properties.push("secret_access_key");
        property_types.push("String");
        property_values.push(params.credentials.secret_access_key.clone());
        property_defaults.push(
            default_stage
                .stage_params
                .credentials
                .secret_access_key
                .clone(),
        );

        // format
        {
            parent_properties.push("file_format");
            properties.push("format");
            property_types.push("String");
            property_values.push(format!("{:?}", stage.file_format.format));
            property_defaults.push(format!("{:?}", default_stage.file_format.format));

            parent_properties.push("file_format");
            properties.push("record_delimiter");
            property_types.push("String");
            property_values.push(stage.file_format.record_delimiter);
            property_defaults.push(default_stage.file_format.record_delimiter);

            parent_properties.push("file_format");
            properties.push("field_delimiter");
            property_types.push("String");
            property_values.push(stage.file_format.field_delimiter.clone());
            property_defaults.push(default_stage.file_format.field_delimiter.clone());

            parent_properties.push("file_format");
            properties.push("csv_header");
            property_types.push("Boolean");
            property_values.push(format!("{:?}", stage.file_format.csv_header));
            property_defaults.push(format!("{:?}", default_stage.file_format.csv_header));

            parent_properties.push("file_format");
            properties.push("compression");
            property_types.push("String");
            property_values.push(format!("{:?}", stage.file_format.compression));
            property_defaults.push(format!("{:?}", default_stage.file_format.compression));
        }

        let property_changed = property_values
            .iter()
            .zip(property_defaults.iter())
            .map(|(v, d)| v != d)
            .collect::<Vec<bool>>();

        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(parent_properties),
            Series::from_data(properties),
            Series::from_data(property_types),
            Series::from_data(property_values),
            Series::from_data(property_defaults),
            Series::from_data(property_changed),
        ]);
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
