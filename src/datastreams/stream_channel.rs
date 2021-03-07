// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::task::{Context, Poll};

use futures::stream::Stream;
use tokio::sync::mpsc::Receiver;

pub struct ChannelStream<T> {
    pub input: Receiver<T>,
}

impl<T> ChannelStream<T> {
    pub fn create(input: Receiver<T>) -> Self {
        Self { input }
    }
}

impl<T> Stream for ChannelStream<T> {
    type Item = T;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self;
        this.input.poll_recv(cx)
    }
}
