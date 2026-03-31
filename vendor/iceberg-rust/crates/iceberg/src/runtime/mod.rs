// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This module contains the async runtime abstraction for iceberg.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task;

pub struct JoinHandle<T>(task::JoinHandle<T>);

impl<T> Unpin for JoinHandle<T> {}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            JoinHandle(handle) => Pin::new(handle)
                .poll(cx)
                .map(|r| r.expect("tokio spawned task failed")),
        }
    }
}

#[allow(dead_code)]
pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle(task::spawn(f))
}

#[allow(dead_code)]
pub fn spawn_blocking<F, T>(f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle(task::spawn_blocking(f))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tokio_spawn() {
        let handle = spawn(async { 1 + 1 });
        assert_eq!(handle.await, 2);
    }

    #[tokio::test]
    async fn test_tokio_spawn_blocking() {
        let handle = spawn_blocking(|| 1 + 1);
        assert_eq!(handle.await, 2);
    }
}
