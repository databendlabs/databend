// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::task::{Context, Poll};

use futures::stream::Stream;
use tokio::sync::mpsc::Receiver;

use crate::datablocks::DataBlock;
use crate::error::FuseQueryResult;

pub struct ChannelStream {
    pub input: Receiver<FuseQueryResult<DataBlock>>,
}

impl Stream for ChannelStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self;
        this.input.poll_recv(cx)
    }
}
