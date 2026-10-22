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

use std::collections::BTreeSet;
use std::result::Result as StdResult;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::crud::CrudError;
use databend_common_meta_api::kv_pb_api::decode_pb;
use databend_common_meta_app::KeyUnknownBuilder;
use databend_common_meta_app::principal::ExternalAccessIntegration;
use databend_common_meta_app::principal::NetworkRule;
use databend_common_meta_app::principal::external_access_integration_ident::Resource as IntegrationResource;
use databend_common_meta_app::principal::network_rule_ident::Resource as RuleResource;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_common_meta_app::tenant_key::ident::TIdent;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MatchSeq;
use url::Url;

use crate::UserApiProvider;

/// HOST_PORT values have no URL scheme, credentials, path or wildcards.
/// Like Snowflake, an omitted port means 443; port 0 / all-ports is not supported.
pub fn network_rule_endpoint(value: &str) -> Result<(String, u16)> {
    if value.is_empty()
        || value.ends_with(':')
        || value
            .chars()
            .any(|c| c.is_whitespace() || "/\\@?#%*".contains(c))
    {
        return Err(ErrorCode::InvalidArgument(format!(
            "Invalid HOST_PORT value: {value}"
        )));
    }
    let url = Url::parse(&format!("https://{value}"))
        .map_err(|_| ErrorCode::InvalidArgument(format!("Invalid HOST_PORT value: {value}")))?;
    endpoint(&url)
}

fn endpoint(url: &Url) -> Result<(String, u16)> {
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ErrorCode::InvalidArgument(
            "Expected an HTTP(S) UDF server address without credentials, query or fragment",
        ));
    }
    match (url.host_str(), url.port_or_known_default()) {
        (Some(host), Some(port)) if port != 0 => Ok((host.to_string(), port)),
        _ => Err(ErrorCode::InvalidArgument(
            "Expected a host and a nonzero port",
        )),
    }
}

impl UserApiProvider {
    pub async fn get_network_rule(
        &self,
        tenant: &Tenant,
        name: &str,
    ) -> StdResult<NetworkRule, CrudError<UnknownError<RuleResource>>> {
        self.network_rule_api(tenant)
            .get(name, MatchSeq::GE(1))
            .await
            .map(|value| value.data)
    }

    // Preserve typed errors so IF EXISTS only suppresses an unknown object.
    pub async fn get_external_access_integration(
        &self,
        tenant: &Tenant,
        name: &str,
    ) -> StdResult<ExternalAccessIntegration, CrudError<UnknownError<IntegrationResource>>> {
        self.external_access_integration_api(tenant)
            .get(name, MatchSeq::GE(1))
            .await
            .map(|value| value.data)
    }

    pub async fn add_network_rule(
        &self,
        tenant: &Tenant,
        rule: NetworkRule,
        create_option: &CreateOption,
    ) -> Result<()> {
        for value in &rule.value_list {
            network_rule_endpoint(value)?;
        }
        self.network_rule_api(tenant)
            .add(rule, create_option)
            .await?;
        Ok(())
    }

    pub async fn update_network_rule(
        &self,
        tenant: &Tenant,
        rule: NetworkRule,
        if_exists: bool,
    ) -> Result<()> {
        for value in &rule.value_list {
            network_rule_endpoint(value)?;
        }
        if let Err(e) = self
            .network_rule_api(tenant)
            .update(rule, MatchSeq::GE(1))
            .await
        {
            if !(if_exists && e.is_business_error()) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub async fn drop_network_rule(
        &self,
        tenant: &Tenant,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        if let Err(e) = self
            .network_rule_api(tenant)
            .remove(name, MatchSeq::GE(1))
            .await
        {
            if !(if_exists && e.is_business_error()) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub async fn add_external_access_integration(
        &self,
        tenant: &Tenant,
        integration: ExternalAccessIntegration,
        create_option: &CreateOption,
    ) -> Result<()> {
        for rule in &integration.allowed_network_rules {
            self.get_network_rule(tenant, rule).await?;
        }
        self.external_access_integration_api(tenant)
            .add(integration, create_option)
            .await?;
        Ok(())
    }

    pub async fn update_external_access_integration(
        &self,
        tenant: &Tenant,
        name: &str,
        rules: Option<Vec<String>>,
        enabled: Option<bool>,
        if_exists: bool,
    ) -> Result<()> {
        if if_exists {
            if let Err(e) = self.get_external_access_integration(tenant, name).await {
                if e.is_business_error() {
                    return Ok(());
                }
                return Err(e.into());
            }
        }
        if let Some(rules) = &rules {
            for rule in rules {
                self.get_network_rule(tenant, rule).await?;
            }
        }
        if let Err(e) = self
            .external_access_integration_api(tenant)
            .cas_with(name, MatchSeq::GE(1), |old| {
                let mut integration = old.data;
                if let Some(rules) = &rules {
                    integration.allowed_network_rules = rules.clone();
                }
                if let Some(enabled) = enabled {
                    integration.enabled = enabled;
                }
                integration
            })
            .await
        {
            if !(if_exists && e.is_business_error()) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub async fn drop_external_access_integration(
        &self,
        tenant: &Tenant,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        if let Err(e) = self
            .external_access_integration_api(tenant)
            .remove(name, MatchSeq::GE(1))
            .await
        {
            if !(if_exists && e.is_business_error()) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Reuse the node-local meta watch caches, falling back to meta when unavailable.
    /// Policy changes follow the existing watch cache's asynchronous propagation.
    /// Missing rules match nothing; explicit integrations never fall back to the
    /// legacy allow list. Callers also enforce the operator's configured ceiling.
    /// Creation checks SUPER and integration USAGE; invocation uses the existing
    /// UDF privileges and does not require integration USAGE.
    pub async fn validate_external_access(
        &self,
        tenant: &Tenant,
        address: &str,
        integrations: &[String],
    ) -> Result<()> {
        if integrations.is_empty() {
            return Err(ErrorCode::InvalidArgument(
                "Expected at least one external access integration",
            ));
        }
        let url = Url::parse(address).map_err(|e| ErrorCode::InvalidArgument(e.to_string()))?;
        let target = endpoint(&url)?;
        let integrations = integrations.iter().collect::<BTreeSet<_>>();
        let keys = integrations
            .iter()
            .map(|name| TIdent::<IntegrationResource>::new(tenant, name).to_string_key());
        let values = self
            .external_access_values(tenant, &self.external_access_integration_cache, keys)
            .await?;
        let mut rules = BTreeSet::new();
        for (name, value) in integrations.iter().zip(values) {
            let value = value.ok_or_else(|| {
                let ident = TIdent::<IntegrationResource>::new(tenant, name);
                ErrorCode::from(ident.unknown_error("validate external access"))
            })?;
            let integration = decode_pb::<ExternalAccessIntegration>(&value.data)
                .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;
            if !integration.enabled {
                return Err(ErrorCode::PermissionDenied(format!(
                    "External access integration '{name}' is disabled"
                )));
            }
            rules.extend(integration.allowed_network_rules);
        }
        let keys = rules
            .iter()
            .map(|name| TIdent::<RuleResource>::new(tenant, name).to_string_key());
        let values = self
            .external_access_values(tenant, &self.network_rule_cache, keys)
            .await?;
        let mut allowed = false;
        for value in values.into_iter().flatten() {
            let rule = decode_pb::<NetworkRule>(&value.data)
                .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;
            for value in &rule.value_list {
                allowed |= network_rule_endpoint(value)? == target;
            }
        }
        if !allowed {
            return Err(ErrorCode::PermissionDenied(format!(
                "UDF server address '{address}' is not allowed by external access integrations"
            )));
        }
        Ok(())
    }
}
