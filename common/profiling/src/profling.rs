// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
//use pprof::protos::Message;

pub struct Profiling {
    duration: Duration,
}

impl Profiling {
    pub fn create(duration: Duration) -> Self {
        Self { duration }
    }

    pub async fn report(&self) -> Result<pprof::Report> {
        // 99 HZ
        let guard = pprof::ProfilerGuard::new(99)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        tokio::time::sleep(self.duration).await;
        guard
            .report()
            .build()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))
    }

    pub async fn dump(&self) -> Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();

        let report = self.report().await?;
        report
            .flamegraph(&mut body)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        /*
        let profile = report
            .pprof()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        profile
            .encode(&mut body)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;

         */

        Ok(body)
    }
}
