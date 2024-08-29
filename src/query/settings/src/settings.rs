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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use dashmap::DashMap;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use itertools::Itertools;
use serde::Deserializer;
use serde::Serializer;

use crate::settings_default::DefaultSettingValue;
use crate::settings_default::DefaultSettings;
use crate::settings_default::SettingRange;
use crate::SettingMode;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub enum ScopeLevel {
    Default,
    Local,
    Global,
    Session,
}

impl Debug for ScopeLevel {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        match self {
            ScopeLevel::Default => {
                write!(f, "DEFAULT")
            }
            ScopeLevel::Local => {
                write!(f, "LOCAL")
            }
            ScopeLevel::Global => {
                write!(f, "GLOBAL")
            }
            ScopeLevel::Session => {
                write!(f, "SESSION")
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug)]
pub struct ChangeValue {
    pub level: ScopeLevel,
    pub value: UserSettingValue,
}

#[derive(Debug)]
pub struct Settings {
    pub(crate) tenant: Tenant,
    pub(crate) changes: Arc<DashMap<String, ChangeValue>>,
    pub(crate) configs: HashMap<String, UserSettingValue>,
}

impl serde::Serialize for Settings {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        #[derive(serde::Serialize)]
        struct SerializeSettings<'a> {
            tenant: &'a String,
            changes: &'a DashMap<String, ChangeValue>,
        }

        let serialize_settings = SerializeSettings {
            tenant: &self.tenant.tenant,
            changes: &self.changes,
        };

        serialize_settings.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Settings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(serde::Deserialize)]
        struct DeserializeSettings {
            tenant: String,
            changes: DashMap<String, ChangeValue>,
        }

        let deserialize_settings =
            <DeserializeSettings as serde::Deserialize>::deserialize(deserializer)?;
        let configs = match GlobalConfig::try_get_instance() {
            None => HashMap::new(),
            Some(config) => config.query.settings.clone(),
        };

        Ok(Settings {
            configs,
            tenant: Tenant::new_literal(&deserialize_settings.tenant),
            changes: Arc::new(deserialize_settings.changes),
        })
    }
}

impl Settings {
    pub fn create(tenant: Tenant) -> Arc<Settings> {
        let configs = match GlobalConfig::try_get_instance() {
            Some(conf) => conf.query.settings.clone(),
            None => HashMap::new(),
        };
        Arc::new(Settings {
            tenant,
            changes: Arc::new(DashMap::new()),
            configs,
        })
    }

    pub fn has_setting(&self, key: &str) -> Result<bool> {
        Ok(DefaultSettings::instance()?.settings.contains_key(key))
    }

    pub fn get_default_value(&self, key: &str) -> Result<Option<UserSettingValue>> {
        let val = DefaultSettings::instance()?
            .settings
            .get(key)
            .map(|v| v.value.clone());
        Ok(val)
    }

    pub fn unset_setting(&self, k: &str) {
        self.changes.remove(k);
    }

    pub fn set_batch_settings(&self, settings: &HashMap<String, String>) -> Result<()> {
        for (k, v) in settings.iter() {
            if self.has_setting(k.as_str())? {
                self.set_setting(k.to_string(), v.to_string())?;
            }
        }

        Ok(())
    }

    pub fn is_changed(&self) -> bool {
        !self.changes.is_empty()
    }

    pub fn changes(&self) -> &Arc<DashMap<String, ChangeValue>> {
        &self.changes
    }

    /// # Safety
    ///
    /// We will not validate the setting value type
    pub unsafe fn unchecked_apply_changes(&self, changes: &DashMap<String, ChangeValue>) {
        for r in changes.iter() {
            let (k, v) = r.pair();
            self.changes.insert(k.clone(), v.clone());
        }
    }
}

pub struct SettingsItem {
    pub name: String,
    pub level: ScopeLevel,
    pub desc: &'static str,
    pub user_value: UserSettingValue,
    pub default_value: UserSettingValue,
    pub range: Option<SettingRange>,
}

pub struct SettingsIter<'a> {
    settings: &'a Settings,
    inner: std::vec::IntoIter<(String, DefaultSettingValue)>,
}

impl<'a> SettingsIter<'a> {
    pub fn create(settings: &'a Settings) -> SettingsIter<'a> {
        let iter = DefaultSettings::instance()
            .unwrap()
            .settings
            .clone()
            .into_iter()
            .sorted_by(|(l, _), (r, _)| Ord::cmp(l, r));

        SettingsIter::<'a> {
            settings,
            inner: iter,
        }
    }
}

impl<'a> Iterator for SettingsIter<'a> {
    type Item = SettingsItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return match self.inner.next() {
                None => None,
                Some((_, value)) if matches!(value.mode, SettingMode::Write) => {
                    continue;
                }
                Some((key, default_value)) => Some(match self.settings.changes.get(&key) {
                    Some(change_value) => SettingsItem {
                        name: key,
                        level: change_value.level.clone(),
                        desc: default_value.desc,
                        user_value: change_value.value.clone(),
                        default_value: default_value.value,
                        range: default_value.range,
                    },
                    None => match self.settings.configs.get(&key) {
                        Some(local_value) => SettingsItem {
                            name: key,
                            level: ScopeLevel::Local,
                            desc: default_value.desc,
                            user_value: local_value.clone(),
                            default_value: default_value.value,
                            range: default_value.range,
                        },
                        None => SettingsItem {
                            name: key,
                            level: ScopeLevel::Default,
                            desc: default_value.desc,
                            user_value: default_value.value.clone(),
                            default_value: default_value.value,
                            range: default_value.range,
                        },
                    },
                }),
            };
        }
    }
}

impl<'a> IntoIterator for &'a Settings {
    type Item = SettingsItem;
    type IntoIter = SettingsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SettingsIter::<'a>::create(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use dashmap::DashMap;
    use databend_common_exception::Result;
    use databend_common_meta_app::principal::UserSettingValue;
    use databend_common_meta_app::tenant::Tenant;

    use crate::ChangeValue;
    use crate::ScopeLevel;
    use crate::Settings;

    #[test]
    fn test_serialize_and_deserialize_settings() -> Result<()> {
        let changes = DashMap::new();
        changes.insert("test_key_1".to_string(), ChangeValue {
            level: ScopeLevel::Default,
            value: UserSettingValue::String("test_value".to_string()),
        });

        let settings = Settings {
            tenant: Tenant::new_literal("test_tenant"),
            changes: Arc::new(changes),
            configs: HashMap::new(),
        };

        let settings =
            serde_json::from_str::<Settings>(&serde_json::to_string(&settings).unwrap()).unwrap();

        assert_eq!(settings.tenant.tenant.as_str(), "test_tenant");
        assert_eq!(settings.changes.len(), 1);
        Ok(())
    }
}
