// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

#[async_trait::async_trait]
pub trait SessionApi {
    async fn kill(&mut self, session_id: String) -> common_exception::Result<()>;
}
