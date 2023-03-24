use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSettingValue;
use dashmap::DashMap;
use itertools::Itertools;

use crate::settings_default::DefaultSettingValue;
use crate::settings_default::DefaultSettings;
use crate::ScopeLevel;

#[derive(Debug)]
pub struct ChangeValue {
    pub level: ScopeLevel,
    pub value: UserSettingValue,
}

#[derive(Debug)]
pub struct NewSettings {
    pub(crate) tenant: String,
    pub(crate) changes: DashMap<String, ChangeValue>,
}

impl NewSettings {
    pub fn try_create(tenant: String) -> Arc<NewSettings> {
        Arc::new(NewSettings {
            tenant,
            changes: DashMap::new(),
        })
    }

    pub fn has_setting(&self, key: &str) -> Result<bool> {
        DefaultSettings::has_setting(key)
    }

    pub fn check_and_get_default_value(&self, key: &str) -> Result<UserSettingValue> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => Ok(v.value.clone()),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn get_setting_level(&self, key: &str) -> Result<ScopeLevel> {
        if let Some(entry) = self.changes.get(key) {
            return Ok(entry.level.clone());
        }

        match DefaultSettings::has_setting(key)? {
            true => Ok(ScopeLevel::Session),
            false => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn set_setting(&self, k: String, v: String) -> Result<()> {
        if let (key, Some(value)) = DefaultSettings::convert_value(k, v)? {
            self.changes.insert(key, ChangeValue {
                value,
                level: ScopeLevel::Session,
            });
        }

        Ok(())
    }

    pub fn set_batch_settings(&self, settings: &HashMap<String, String>) -> Result<()> {
        for (k, v) in settings.iter() {
            if self.has_setting(k.as_str())? {
                self.set_setting(k.to_string(), v.to_string())?;
            }
        }

        Ok(())
    }
}

pub struct SettingsItem {
    pub name: String,
    pub level: ScopeLevel,
    pub desc: &'static str,
    pub user_value: UserSettingValue,
    pub default_value: UserSettingValue,
    pub possible_values: Option<Vec<&'static str>>,
}

pub struct SettingsIter<'a> {
    settings: &'a NewSettings,
    inner: std::vec::IntoIter<(String, DefaultSettingValue)>,
}

impl<'a> SettingsIter<'a> {
    pub fn create(settings: &'a NewSettings) -> SettingsIter<'a> {
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
        match self.inner.next() {
            None => None,
            Some((key, default_value)) => Some(match self.settings.changes.get(&key) {
                None => SettingsItem {
                    name: key,
                    level: ScopeLevel::Session,
                    desc: default_value.desc,
                    user_value: default_value.value.clone(),
                    default_value: default_value.value,
                    possible_values: default_value.possible_values,
                },
                Some(change_value) => SettingsItem {
                    name: key,
                    level: change_value.level.clone(),
                    desc: default_value.desc,
                    user_value: change_value.value.clone(),
                    default_value: default_value.value,
                    possible_values: default_value.possible_values,
                },
            }),
        }
    }
}

impl<'a> IntoIterator for &'a NewSettings {
    type Item = SettingsItem;
    type IntoIter = SettingsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SettingsIter::<'a>::create(self)
    }
}
