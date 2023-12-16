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

/// Compatible layer to receive different types of errors from meta-service.
///
/// It allows the server side to switch to return a smaller error type, e.g., from KVAppError to MetaAPIError.
///
/// Currently:
/// - Meta service kv_api returns KVAppError, while the client only consume the MetaApiError variants
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum Compatible<Outer, Inner>
where
    Outer: From<Inner>,
    Outer: TryInto<Inner>,
{
    Outer(Outer),
    Inner(Inner),
}

impl<Outer, Inner> Compatible<Outer, Inner>
where
    Outer: From<Inner>,
    Outer: TryInto<Inner>,
{
    pub fn into_inner(self) -> Inner
    where Inner: From<<Outer as TryInto<Inner>>::Error> {
        match self {
            Compatible::Outer(o) => {
                let i: Inner = o.try_into().unwrap_or_else(|e| Inner::from(e));
                i
            }
            Compatible::Inner(i) => i,
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_types::ForwardToLeader;
    use databend_common_meta_types::MembershipNode;
    use databend_common_meta_types::MetaAPIError;
    use databend_common_meta_types::MetaError;

    use crate::compat_errors::Compatible;
    use crate::kv_app_error::KVAppError;

    #[test]
    fn test_read_api_err_from_api_err() -> anyhow::Result<()> {
        let me = MetaAPIError::ForwardToLeader(ForwardToLeader {
            leader_id: Some(1),
            leader_node: Some(MembershipNode {}),
        });
        let s = serde_json::to_string(&me)?;

        let ge: Compatible<KVAppError, MetaAPIError> = serde_json::from_str(&s)?;

        if let MetaAPIError::ForwardToLeader(f) = ge.clone().into_inner() {
            assert_eq!(Some(1), f.leader_id);
        } else {
            unreachable!("expect ForwardToLeader but: {:?}", ge);
        }

        Ok(())
    }

    #[test]
    fn test_read_api_err_from_kv_app_err() -> anyhow::Result<()> {
        let me = KVAppError::MetaError(MetaError::APIError(MetaAPIError::ForwardToLeader(
            ForwardToLeader {
                leader_id: Some(1),
                leader_node: Some(MembershipNode {}),
            },
        )));
        let s = serde_json::to_string(&me)?;

        let ge: Compatible<KVAppError, MetaAPIError> = serde_json::from_str(&s)?;

        if let MetaAPIError::ForwardToLeader(f) = ge.clone().into_inner() {
            assert_eq!(Some(1), f.leader_id);
        } else {
            unreachable!("expect ForwardToLeader but: {:?}", ge);
        }

        Ok(())
    }
}
