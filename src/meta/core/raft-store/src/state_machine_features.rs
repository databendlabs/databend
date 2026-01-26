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

//! Runtime feature toggles for state machine behavior.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;
use strum::IntoEnumIterator;

/// Features that implemented by `StateMachine` that can be enabled or disabled.
///
/// To enable/disable a feature with:
/// `POST /api/v1/feature?feature=dummy_feature2&enable=true`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, strum_macros::EnumIter)]
#[serde(rename_all = "snake_case")]
pub enum StateMachineFeature {
    /// Enables nothing, for testing only.
    Dummy,
    /// Enable another dummy feature
    DummyFeature2,
}

impl StateMachineFeature {
    pub fn all() -> Vec<Self> {
        Self::iter().collect()
    }
}

impl fmt::Display for StateMachineFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json_str = serde_json::to_string(self).map_err(|_| fmt::Error)?;
        // strip the quotes from the serialized string
        let trimmed = json_str.trim_matches('"');
        write!(f, "{}", trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::StateMachineFeature;

    #[test]
    fn test_display() {
        let expected = ["dummy", "dummy_feature2"];
        for (i, feat) in StateMachineFeature::all().into_iter().enumerate() {
            let feat_str = feat.to_string();
            let expected_str = expected[i];
            assert_eq!(
                feat_str, expected_str,
                "Expected: {}, got: {}",
                expected_str, feat_str
            );
        }
    }

    #[test]
    fn test_serde() {
        let feature = StateMachineFeature::DummyFeature2;
        let serialized = serde_json::to_string(&feature).unwrap();
        assert_eq!(serialized, "\"dummy_feature2\"");

        let deserialized: StateMachineFeature = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, feature);

        let all_features = StateMachineFeature::all();
        assert_eq!(all_features, vec![
            StateMachineFeature::Dummy,
            StateMachineFeature::DummyFeature2
        ]);
    }
}
