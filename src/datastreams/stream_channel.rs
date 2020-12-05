// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::stream::Stream;
use tokio::sync::mpsc::Receiver;

use crate::datablocks::DataBlock;
use crate::error::FuseQueryResult;

pin_project! {
    pub struct ChannelStream {
        #[pin]
        pub input: Receiver<FuseQueryResult<DataBlock>>,
    }
}

impl Stream for ChannelStream {
    type Item = FuseQueryResult<DataBlock>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.input.poll_next(cx)
    }
}
