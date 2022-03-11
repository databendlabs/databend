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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use common_streams::SortStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct SortPartialTransform {
    schema: DataSchemaRef,
    exprs: Vec<Expression>,
    limit: Option<usize>,
    input: Arc<dyn Processor>,
}

impl SortPartialTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        exprs: Vec<Expression>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(SortPartialTransform {
            schema,
            exprs,
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl Processor for SortPartialTransform {
    fn name(&self) -> &str {
        "SortPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name = "sort_partial_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        Ok(Box::pin(SortStream::try_create(
            self.input.execute().await?,
            get_sort_descriptions(&self.schema, &self.exprs)?,
            self.limit,
        )?))
    }
}

pub fn get_sort_descriptions(
    schema: &DataSchemaRef,
    exprs: &[Expression],
) -> Result<Vec<SortColumnDescription>> {
    let mut sort_columns_descriptions = vec![];
    for x in exprs {
        match *x {
            Expression::Sort {
                ref expr,
                asc,
                nulls_first,
                ..
            } => {
                let column_name = expr.to_data_field(schema)?.name().clone();
                sort_columns_descriptions.push(SortColumnDescription {
                    column_name,
                    asc,
                    nulls_first,
                });
            }
            _ => {
                return Result::Err(ErrorCode::BadTransformType(format!(
                    "Sort expression must be ExpressionPlan::Sort, but got: {:?}",
                    x
                )));
            }
        }
    }
    Ok(sort_columns_descriptions)
}
