// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use common_datablocks::DataBlock;
use common_datavalues::DataArrayRef;
use common_datavalues::DataSchemaRef;
use common_functions::IFunction;
use common_streams::ExpressionStream;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

pub struct ProjectionTransform {
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl ProjectionTransform {
    pub fn try_create(schema: DataSchemaRef) -> Result<Self> {
        Ok(ProjectionTransform {
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn projection_executor(
        projected_schema: &DataSchemaRef,
        block: DataBlock,
        _funcs: Vec<Box<dyn IFunction>>,
    ) -> Result<DataBlock> {
        let mut columns: Vec<DataArrayRef> = Vec::with_capacity(projected_schema.fields().len());

        for field in projected_schema.fields() {
            let column = block.column_by_name(field.name())?;
            columns.push(column.clone());
        }
        Ok(DataBlock::create(projected_schema.clone(), columns))
    }
}

#[async_trait::async_trait]
impl IProcessor for ProjectionTransform {
    fn name(&self) -> &str {
        "ProjectionTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            self.schema.clone(),
            vec![],
            ProjectionTransform::projection_executor,
        )?))
    }
}
