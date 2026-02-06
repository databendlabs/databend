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

#![allow(
    clippy::collapsible_if,
    clippy::let_and_return,
    clippy::manual_is_multiple_of
)]

pub mod attach_table;
pub mod data_mask;
pub mod enterprise_services;
pub mod fail_safe;
pub mod hilbert_clustering;
pub mod license;
pub mod resource_management;
pub mod row_access_policy;
pub mod storage_encryption;
pub mod storages;
pub mod stream;
pub mod table_ref;
pub mod test_kits;
pub mod virtual_column;

/// Convert a meta service error to an ErrorCode.
pub(crate) fn meta_service_error(
    e: databend_meta_types::MetaError,
) -> databend_common_exception::ErrorCode {
    databend_common_exception::ErrorCode::MetaServiceError(e.to_string())
}
