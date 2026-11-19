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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use tokio_util::sync::CancellationToken;

use super::MaintenanceContext;
use super::MaintenanceIdentity;
use super::MaintenanceResources;
use super::resources::MaintenanceAdmission;
use super::run_maintenance_unit;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub(crate) enum Execution<T> {
    Executed(T),
    Busy,
    Closed,
    Panicked(ErrorCode),
}

/// Busy is local admission deferral, not an execution OOM. Every executed unit has
/// fresh identity validation, tracking, context and bounded cooperative cancellation.
/// Cancellation retains the existing pipeline semantics; it is not a worker join.
/// Neither a queued task nor its continuation owns any of these execution resources.
pub(crate) async fn execute_unit<T, F: Future<Output = Result<T>>>(
    identity: &MaintenanceIdentity,
    cancellation: CancellationToken,
    resources: &Arc<MaintenanceResources>,
    demand: usize,
    work: impl FnOnce(Arc<QueryContext>) -> F,
) -> Result<Execution<T>> {
    if cancellation.is_cancelled() {
        return Err(ErrorCode::AbortedQuery("Maintenance scheduler closed"));
    }
    let reservation = match resources.try_reserve(demand) {
        MaintenanceAdmission::Granted(reservation) => reservation,
        MaintenanceAdmission::Busy => return Ok(Execution::Busy),
        MaintenanceAdmission::Closed => return Ok(Execution::Closed),
    };
    let MaintenanceContext { ctx, tracking } = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(ErrorCode::AbortedQuery("Maintenance scheduler closed")),
        context = tokio::time::timeout(
            Duration::from_secs(60),
            MaintenanceContext::from_identity_with_reservation(identity, reservation),
        ) => context.map_err(|_| ErrorCode::AbortedQuery("Maintenance context creation timed out"))??,
    };
    let result = CatchUnwindFuture::create(async {
        run_maintenance_unit(&ctx, cancellation, tracking.tracking(work(ctx.clone()))).await
    })
    .await;
    match result {
        Ok(result) => result.map(Execution::Executed),
        Err(error) => Ok(Execution::Panicked(
            error.with_context("automatic maintenance execution panicked"),
        )),
    }
}
