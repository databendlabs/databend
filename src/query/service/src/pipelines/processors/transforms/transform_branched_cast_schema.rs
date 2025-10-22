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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::SourceSchemaIndex;
use databend_common_pipeline_transforms::columns::cast_schema;
use databend_common_pipeline_transforms::Transform;
use databend_common_storages_fuse::TableContext;

use crate::sessions::QueryContext;

/// The key of branches is `SourceSchemaIndex`, see `TransformResortAddOnWithoutSourceSchema`.
pub struct TransformBranchedCastSchema {
    pub ctx: Arc<QueryContext>,
    pub branches: Arc<HashMap<SourceSchemaIndex, CastSchemaBranch>>,
}

pub struct CastSchemaBranch {
    pub to_schema: DataSchemaRef,
    pub from_schema: DataSchemaRef,
    pub exprs: Vec<Expr>,
}

impl Transform for TransformBranchedCastSchema {
    const NAME: &'static str = "BranchedCastSchema";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        // see the comment details of `TransformResortAddOnWithoutSourceSchema`.
        if block.get_meta().is_none() {
            return Ok(block);
        }
        let input_schema_idx =
            SourceSchemaIndex::downcast_from(block.clone().get_owned_meta().unwrap()).unwrap();
        let Some(branch) = self.branches.get(&input_schema_idx) else {
            // no cast schema to execute in this branch, just return the original block
            return Ok(block);
        };

        let CastSchemaBranch {
            to_schema,
            from_schema,
            exprs,
        } = branch;

        let block = cast_schema(
            block,
            from_schema.clone(),
            to_schema.clone(),
            exprs,
            &self.ctx.get_function_context()?,
        )?;
        let source_schema_idx: SourceSchemaIndex = input_schema_idx;
        block.add_meta(Some(Box::new(source_schema_idx)))
    }
}
