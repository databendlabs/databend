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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::aggregate_function::AggregateStateSettings;
use databend_common_expression::aggregate_function::AggregateStateSettingsSelector;
use databend_common_expression::aggregate_function::AggregateStateWritePolicy;
use databend_common_expression::aggregate_function::EXECUTION_ONLY_STATE_VERSION;
use databend_common_expression::aggregate_function::RawAggregateCall;

pub(super) struct CompatibleStateSettings;

impl CompatibleStateSettings {
    fn versioned(request: &RawAggregateCall<'_>, version: u64) -> Result<AggregateStateSettings> {
        if version == 0 {
            Ok(AggregateStateSettings::v0_compatibility())
        } else {
            Err(ErrorCode::BadDataValueType(format!(
                "Aggregate state version {version} is not supported by {} (expected version 0)",
                request.name
            )))
        }
    }

    fn execution_settings(request: &RawAggregateCall<'_>) -> AggregateStateSettings {
        AggregateStateSettings {
            state_version: EXECUTION_ONLY_STATE_VERSION,
            preserve_nullable_input_rows_flag: request.distinct,
            input_nullable_input_rows_flag: request.distinct,
        }
    }
}

impl AggregateStateSettingsSelector for CompatibleStateSettings {
    fn execution(&self, request: &RawAggregateCall<'_>) -> Result<AggregateStateSettings> {
        Ok(Self::execution_settings(request))
    }

    fn read(
        &self,
        request: &RawAggregateCall<'_>,
        input_version: Option<u64>,
    ) -> Result<AggregateStateSettings> {
        let Some(input_version) = input_version else {
            // Legacy physical states carry no format metadata; read them in
            // the v0 layout without assigning a persisted output version.
            return Ok(AggregateStateSettings {
                state_version: EXECUTION_ONLY_STATE_VERSION,
                preserve_nullable_input_rows_flag: true,
                input_nullable_input_rows_flag: true,
            });
        };
        let input = Self::versioned(request, input_version)?;
        let mut settings = Self::execution_settings(request);
        settings.input_nullable_input_rows_flag = input.preserve_nullable_input_rows_flag;
        Ok(settings)
    }

    fn write(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
    ) -> Result<AggregateStateSettings> {
        let version = match policy {
            AggregateStateWritePolicy::Compatible => 0,
            AggregateStateWritePolicy::Latest => 1,
        };
        Self::versioned(request, version)
    }

    fn rewrite(
        &self,
        request: &RawAggregateCall<'_>,
        policy: AggregateStateWritePolicy,
        input_version: Option<u64>,
    ) -> Result<AggregateStateSettings> {
        let mut settings = self.write(request, policy)?;
        if let Some(version) = input_version {
            let input = Self::versioned(request, version)?;
            settings.input_nullable_input_rows_flag = input.preserve_nullable_input_rows_flag;
        }
        Ok(settings)
    }
}
