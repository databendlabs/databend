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

//! Defines the name space, key and value type for leveled store.

use map_api::mvcc;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Namespace {
    User,
    Expire,
}

impl mvcc::ViewNameSpace for Namespace {
    fn if_increase_seq(&self) -> bool {
        match self {
            Namespace::User => true,
            // Backward compatibility: when inserting an expiry index as the secondary index, do not increase seq.
            Namespace::Expire => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    User(UserKey),
    Expire(ExpireKey),
}

impl Key {
    pub fn into_user(self) -> UserKey {
        match self {
            Key::User(k) => k,
            Key::Expire(_) => unreachable!("expect UserKey, got ExpireKey"),
        }
    }

    pub fn into_expire(self) -> ExpireKey {
        match self {
            Key::User(_) => unreachable!("expect ExpireKey, got UserKey"),
            Key::Expire(k) => k,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    User(MetaValue),
    Expire(String),
}

impl Value {
    pub fn into_user(self) -> MetaValue {
        match self {
            Value::User(v) => v,
            Value::Expire(_) => unreachable!("expect MetaValue, got String"),
        }
    }

    pub fn into_expire(self) -> String {
        match self {
            Value::User(_) => unreachable!("expect String, got MetaValue"),
            Value::Expire(v) => v,
        }
    }
}
