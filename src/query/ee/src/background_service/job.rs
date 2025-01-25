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
use databend_common_exception::Result;
use databend_common_meta_app::background::BackgroundJobIdent;
use databend_common_meta_app::background::BackgroundJobInfo;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobStatus;
use databend_common_meta_types::SeqV;

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
///     async fn run(&mut self) {
///         do_compaction_job().await?;
///     }
///
/// }
/// ```
#[async_trait]
pub trait Job: JobClone {
    /// Runs the job
    async fn run(&mut self);
    fn get_name(&self) -> BackgroundJobIdent;
    async fn get_info(&self) -> Result<SeqV<BackgroundJobInfo>>;
    async fn update_job_status(&mut self, status: BackgroundJobStatus) -> Result<()>;
    async fn update_job_params(&mut self, param: BackgroundJobParams) -> Result<()>;
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
