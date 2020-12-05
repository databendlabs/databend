// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::task::{Context, Poll};

use futures::stream::{Stream, StreamExt};

use crate::datablocks::DataBlock;
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::functions::Function;

pub struct AggregateStream {
    schema: DataSchemaRef,
    exprs: Vec<Function>,
    input: SendableDataBlockStream,
    finished: bool,
}

impl AggregateStream {
    pub fn try_create(
        input: SendableDataBlockStream,
        schema: DataSchemaRef,
        exprs: Vec<Function>,
    ) -> FuseQueryResult<Self> {
        Ok(AggregateStream {
            input,
            schema,
            exprs,
            finished: false,
        })
    }
}

impl Stream for AggregateStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if !self.finished {
            let mut funcs = self.exprs.clone();

            while let Poll::Ready(Some(v)) = self.input.poll_next_unpin(ctx) {
                let v = v?;
                for func in &mut funcs {
                    func.eval(&v)?;
                }
            }
            self.finished = true;
            let mut arrays = Vec::with_capacity(funcs.len());
            for func in &funcs {
                arrays.push(func.result()?.to_array(1)?);
            }
            let block = DataBlock::create(self.schema.clone(), arrays);
            Poll::Ready(Some(Ok(block)))
        } else {
            Poll::Ready(None)
        }
    }
}
