// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use pprof::protos::Message;

pub struct Profiling {
    freq: i32,
}

impl Profiling {
    pub fn create(freq: i32) -> Self {
        Self { freq }
    }

    pub async fn report(&self) -> Result<pprof::Report> {
        let guard = pprof::ProfilerGuard::new(self.freq)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        tokio::time::sleep(Duration::from_secs(self.freq as u64)).await;
        guard
            .report()
            .build()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))
    }

    pub async fn dump(&self) -> Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();

        let report = self.report().await?;
        let profile = report
            .pprof()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        profile
            .encode(&mut body)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;

        Ok(body)
    }
}
