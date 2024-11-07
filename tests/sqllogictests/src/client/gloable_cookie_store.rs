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

use cookie::Cookie;
use reqwest::cookie::CookieStore;
use reqwest::header::HeaderValue;
use std::collections::HashMap;
use std::sync::RwLock;
use url::Url;

pub(crate) struct GlobalCookieStore {
    cookies: RwLock<HashMap<String, Cookie<'static>>>,
}

impl GlobalCookieStore {
    pub fn new() -> Self {
        GlobalCookieStore {
            cookies: RwLock::new(HashMap::new()),
        }
    }
}

impl CookieStore for GlobalCookieStore {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, _url: &Url) {
        let iter = cookie_headers
            .filter_map(|val| std::str::from_utf8(val.as_bytes()).ok())
            .filter_map(|kv| Cookie::parse(kv).map(|c| c.into_owned()).ok());

        let mut guard = self.cookies.write().unwrap();
        for cookie in iter {
            guard.insert(cookie.name().to_string(), cookie);
        }
    }

    fn cookies(&self, _url: &Url) -> Option<HeaderValue> {
        let guard = self.cookies.read().unwrap();
        let s: String = guard
            .values()
            .map(|cookie| cookie.name_value())
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");

        if s.is_empty() {
            return None;
        }

        HeaderValue::from_str(&s).ok()
    }
}
