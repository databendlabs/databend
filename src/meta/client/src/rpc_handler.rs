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

use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::future::TimedFutureExt;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaNetworkError;
use display_more::DisplayOptionExt;
use log::debug;
use log::info;
use log::warn;
use tonic::Code;
use tonic::Response;
use tonic::Status;

use crate::established_client::EstablishedClient;
use crate::FeatureSpec;
use crate::MetaGrpcClient;

/// Represents the action to take after processing an RPC response.
///
/// This enum indicates whether the RPC should be retried due to a transient error,
/// or if it succeeded and should return the response.
#[derive(Debug, Clone)]
pub(crate) enum ResponseAction<T> {
    /// The RPC failed with a retryable error and should be attempted again
    ShouldRetry,
    /// The RPC succeeded and contains the response data
    Success(T),
}

/// Handles RPC communication with the meta-service, including connection management,
/// error tracking, and retry logic.
///
/// This handler manages the lifecycle of RPC calls to the meta-service, tracking
/// failed attempts, establishing connections, and determining when operations
/// should be retried vs. when they should fail permanently.
///
/// # Example
/// ```ignore
/// let mut handler = RpcHandler::new(&client, feature_spec);
/// let established = handler.new_established_client().await?;
/// let result = handler.process_response_result(&request, rpc_result)?;
/// ```
pub(crate) struct RpcHandler<'a> {
    /// Reference to the gRPC client for meta-service communication
    pub(crate) client: &'a MetaGrpcClient,

    /// History of failed RPC attempts, storing endpoint description and error details.
    /// Used for error reporting and debugging connection issues.
    pub(crate) rpc_failures: Vec<(String, Status)>,

    /// Feature requirements that must be supported by the meta-service.
    /// Used to validate compatibility before executing RPCs.
    pub(crate) required_feature: FeatureSpec,

    /// Currently established connection to the meta-service, if any.
    /// This is populated after a successful connection is made and validated.
    pub(crate) established_client: Option<EstablishedClient>,
}

impl<'a> RpcHandler<'a> {
    /// Creates a new RPC handler for the given client and feature requirements.
    pub(crate) fn new(client: &'a MetaGrpcClient, required_feature: FeatureSpec) -> Self {
        Self {
            client,
            rpc_failures: Vec::new(),
            required_feature,
            established_client: None,
        }
    }

    /// Establishes a new connection to the meta-service and validates feature compatibility.
    ///
    /// This method will:
    /// 1. Get an established client from the underlying gRPC client
    /// 2. Validate that the server supports the required features
    /// 3. Store the validated client for future use
    ///
    /// # Returns
    /// A mutable reference to the established client that can be used for RPCs
    ///
    /// # Errors
    /// Returns `MetaClientError` if:
    /// - Connection establishment fails
    /// - Feature validation fails
    /// - Server doesn't support required features
    pub(crate) async fn new_established_client(
        &mut self,
    ) -> Result<&mut EstablishedClient, MetaClientError> {
        let client = self
            .client
            .get_established_client()
            .with_timing_threshold(
                default_timing_threshold(),
                create_timing_logger("MetaGrpcClient::get_established_client"),
            )
            .await?;

        client.ensure_feature_spec(&self.required_feature)?;

        self.established_client = Some(client);

        Ok(self.established_client.as_mut().unwrap())
    }

    /// Processes an RPC response and determines the appropriate action.
    ///
    /// This method analyzes the RPC result and decides whether to:
    /// - Retry the operation (for transient errors)
    /// - Return success (for successful responses)
    /// - Fail permanently (for non-retryable errors)
    ///
    /// For retryable errors, it will:
    /// - Log the failure
    /// - Record the failure for error reporting
    /// - Switch to the next endpoint for retry
    ///
    /// # Returns
    /// - `Ok(ResponseAction::ShouldRetry)` for retryable errors
    /// - `Ok(ResponseAction::Success(response))` for successful responses
    /// - `Err(status)` for non-retryable errors
    pub(crate) fn process_response_result<R, T>(
        &mut self,
        request: &R,
        result: Result<Response<T>, Status>,
    ) -> Result<ResponseAction<Response<T>>, Status>
    where
        R: fmt::Debug,
        T: fmt::Debug,
    {
        debug!(
            "MetaGrpcClient::{} {}-th try: result: {:?}",
            self.required_feature.0,
            self.rpc_failures.len(),
            result
        );

        let status = match result {
            Err(e) => e,
            Ok(x) => return Ok(ResponseAction::Success(x)),
        };

        if is_status_retryable(&status) {
            warn!(
                "MetaGrpcClient::{} retryable error: {:?}; with {}: request: {:?}",
                self.required_feature.0,
                status,
                self.established_client.display(),
                request
            );

            let established_client = self
                .established_client
                .as_mut()
                .expect("established client should be set before processing response");

            self.rpc_failures
                .push((established_client.to_string(), status));

            established_client.rotate_failing_target();

            Ok(ResponseAction::ShouldRetry)
        } else {
            warn!(
                "MetaGrpcClient::{} non-retryable error: {:?}; with {}: request: {:?}",
                self.required_feature.0,
                status,
                self.established_client.display(),
                request
            );

            Err(status)
        }
    }

    /// Creates a comprehensive network error after all retry attempts have failed.
    ///
    /// This method aggregates all the connection failures that occurred during
    /// retry attempts and creates a detailed error message that includes:
    /// - The number of retry attempts made
    /// - Details of each failure
    /// - The RPC operation that was being attempted
    pub(crate) fn create_network_error(&self) -> MetaNetworkError {
        let conn_err = ConnectionError::new(
            AnyError::error(format_args!(
                "after {} retries: {:?}",
                self.rpc_failures.len(),
                self.rpc_failures
            )),
            format!(
                "failed to send RPC '{}' to meta-service",
                self.required_feature.0
            ),
        );

        MetaNetworkError::ConnectionError(conn_err)
    }
}

/// Returns the default timing threshold for RPC operations.
fn default_timing_threshold() -> Duration {
    Duration::from_millis(300)
}

/// Creates a closure that logs timing information for RPC operations.
fn create_timing_logger<T>(msg: impl Display) -> impl Fn(&T, Duration, Duration) {
    move |_output, total, busy| {
        info!("{} spent: total: {:?}, busy: {:?}", msg, total, busy);
    }
}

/// Determines if a gRPC status code indicates a retryable error.
fn is_status_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::Unavailable | Code::Internal | Code::Cancelled
    )
}
