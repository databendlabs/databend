// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use futures::stream::{Stream, StreamExt};
use std::task::{Context, Poll};

use crate::datablocks::DataBlock;
use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::functions::Function;

pub struct ExpressionStream {
    input: SendableDataBlockStream,
    expr: Option<Function>,
    func: fn(DataBlock, Function) -> FuseQueryResult<DataBlock>,
}

impl ExpressionStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        expr: Option<Function>,
        func: fn(DataBlock, Function) -> FuseQueryResult<DataBlock>,
    ) -> FuseQueryResult<Self> {
        Ok(ExpressionStream { input, expr, func })
    }
}

impl Stream for ExpressionStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(ctx).map(|x| match x {
            Some(Ok(v)) => Some(match &self.expr {
                None => Ok(v),
                Some(expr) => (self.func)(v, expr.clone()),
            }),
            other => other,
        })
    }
}
