use crate::SendableDataBlockStream;
use common_datavalues::{DataSchemaRef, DataField};
use futures::{Stream, StreamExt};
use common_datablocks::DataBlock;
use std::pin::Pin;
use futures::task::{Context, Poll};
use common_exception::{Result, ErrorCode};

pub struct CorrectWithSchemaStream {
    input: SendableDataBlockStream,
    schema: DataSchemaRef,
}

impl CorrectWithSchemaStream {
    pub fn new(input: SendableDataBlockStream, schema: DataSchemaRef) -> Self {
        CorrectWithSchemaStream {
            input,
            schema,
        }
    }

    fn new_block_if_need(&self, data_block: DataBlock) -> Result<DataBlock> {
        match self.schema.eq(data_block.schema()) {
            true => Ok(data_block),
            false => self.new_data_block(data_block)
        }
    }

    fn new_data_block(&self, data_block: DataBlock) -> Result<DataBlock> {
        let schema_fields = self.schema.fields();
        let mut new_columns = Vec::with_capacity(schema_fields.len());
        for schema_field in schema_fields {
            match data_block.column_by_name(schema_field.name()) {
                Some(column) if &column.data_type() == schema_field.data_type() => {
                    new_columns.push(column.clone())
                }
                other => return Err(ErrorCode::IllegalSchema(format!(
                    "Illegal schema. expect: {:?} found: {:?}", schema_field, other
                ))),
            };
        }

        Ok(DataBlock::create(self.schema.clone(), new_columns))
    }
}

impl Stream for CorrectWithSchemaStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|mut x| match x {
            Some(Ok(block)) => Some(self.new_block_if_need(block)),
            other => other,
        })
    }
}
