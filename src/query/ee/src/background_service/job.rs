// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;

use crate::background_service::configs::JobConfig;
/// A trait for implementing a background job
///
/// Example implementation:
///
/// ```ignore
/// use std::time::Duration;
///
/// #[derive(Clone)]
/// struct CompactionJob {
///     config: JobConfig,
/// }
///
/// #[async_trait]
/// impl Job for CompactionJob {
///     async fn run(&self) {
///         do_compaction_job().await?;
///     }
///
///     fn get_config(&self) -> &JobConfig {
///         &self.config
///     }
/// }
/// ```
#[async_trait]
pub trait Job: JobClone {
    /// Runs the job
    async fn run(&self);
    /// Exposes the configuration of the job
    fn get_config(&self) -> &JobConfig;
}

pub trait JobClone {
    fn box_clone(&self) -> BoxedJob;
}

impl<T> JobClone for T
where T: 'static + Job + Clone + Send + Sync
{
    fn box_clone(&self) -> BoxedJob {
        Box::new((*self).clone())
    }
}

impl Clone for Box<dyn Job> {
    fn clone(&self) -> Box<dyn Job> {
        self.box_clone()
    }
}

pub(crate) type BoxedJob = Box<dyn Job + Send + Sync>;
