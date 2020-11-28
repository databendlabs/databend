// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use futures::stream::{Stream, StreamExt};
use std::task::{Context, Poll};

use crate::datablocks::DataBlock;
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::functions::Function;

pub struct ScalarExpressionStream {
    input: SendableDataBlockStream,
    schema: DataSchemaRef,
    exprs: Vec<Function>,
    func: fn(&DataSchemaRef, DataBlock, Vec<Function>) -> FuseQueryResult<DataBlock>,
}

impl ScalarExpressionStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        schema: DataSchemaRef,
        exprs: Vec<Function>,
        func: fn(&DataSchemaRef, DataBlock, Vec<Function>) -> FuseQueryResult<DataBlock>,
    ) -> FuseQueryResult<Self> {
        Ok(ScalarExpressionStream {
            input,
            schema,
            exprs,
            func,
        })
    }
}

impl Stream for ScalarExpressionStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some((self.func)(&self.schema, v, self.exprs.clone())),
            other => other,
        })
    }
}
