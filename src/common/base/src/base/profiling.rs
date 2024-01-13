// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use pprof::protos::Message;

pub struct Profiling {
    duration: Duration,
    frequency: i32,
}

impl Profiling {
    pub fn create(duration: Duration, frequency: i32) -> Self {
        Self {
            duration,
            frequency,
        }
    }

    pub async fn report(&self) -> Result<pprof::Report> {
        let guard = pprof::ProfilerGuard::new(self.frequency)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        tokio::time::sleep(self.duration).await;
        guard
            .report()
            .build()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))
    }

    pub async fn dump_flamegraph(&self) -> Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();

        let report = self.report().await?;
        report
            .flamegraph(&mut body)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;

        Ok(body)
    }

    pub async fn dump_proto(&self) -> Result<Vec<u8>> {
        let mut body: Vec<u8> = Vec::new();

        let report = self.report().await?;
        let profile = report
            .pprof()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;
        profile
            .write_to_vec(&mut body)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?;

        Ok(body)
    }
}
