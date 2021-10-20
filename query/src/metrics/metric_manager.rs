// Copyright 2020 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use metrics_exporter_prometheus::PrometheusRecorder;

pub struct MetricManager {
    recorder: Option<PrometheusRecorder>,
    handle: PrometheusHandle,
}

impl MetricManager {
    pub fn create() -> Self {
        // TODO: add some config options for prometheus, like bucket, etc.
        let builder = PrometheusBuilder::new();
        let recorder = builder.build();
        let handle = recorder.handle();
        Self {
            recorder: Some(recorder),
            handle,
        }
    }

    pub fn handle(&self) -> PrometheusHandle {
        self.handle.clone()
    }

    pub fn install_recorder(&mut self) -> Result<()> {
        let recorder = match self.recorder.take() {
            None => {
                return Err(ErrorCode::InitPrometheusFailure(
                    "Already installed prometheus recorder",
                ))
            }
            Some(recorder) => recorder,
        };
        metrics::clear_recorder();
        match metrics::set_boxed_recorder(Box::new(recorder)) {
            Ok(_) => Ok(()),
            Err(error) => Err(ErrorCode::InitPrometheusFailure(format!(
                "Cannot init prometheus recorder. cause: {}",
                error
            ))),
        }
    }

    pub fn install_recorder_or_warn(&mut self) {
        match self.install_recorder() {
            Ok(()) => (),
            Err(err) => tracing::warn!("Install prometheus recorder failed: {}", err),
        }
    }
}
