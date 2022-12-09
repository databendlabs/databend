// Copyright 2022 Datafuse Labs.
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
use std::sync::Mutex;

use once_cell::sync::OnceCell;
use state::Container;

/// Singleton is a wrapper enum for `Container![Send + Sync]`.
///
/// - `Production` is used in our production code.
/// - `Testing` is served for test only and gated under `debug_assertions`.
pub enum Singleton {
    Production(Container![Send + Sync]),

    #[cfg(debug_assertions)]
    Testing(Mutex<HashMap<String, Container![Send + Sync]>>),
}

unsafe impl Send for Singleton {}
unsafe impl Sync for Singleton {}

impl Singleton {
    fn get<T: Clone + 'static>(&self) -> T {
        match self {
            Singleton::Production(c) => {
                let v: &T = c.get();
                v.clone()
            }
            #[cfg(debug_assertions)]
            Singleton::Testing(c) => {
                let thread = std::thread::current();
                let thread_name = match thread.name() {
                    Some(name) => name,
                    None => panic!("thread doesn't have name"),
                };
                let guard = c.lock().expect("lock must succeed");
                let v: &T = guard
                    .get(thread_name)
                    .expect("thread {name} is not initiated")
                    .get();
                v.clone()
            }
        }
    }

    fn set<T: Send + Sync + 'static>(&self, value: T) -> bool {
        match self {
            Singleton::Production(c) => c.set(value),
            #[cfg(debug_assertions)]
            Singleton::Testing(c) => {
                let thread = std::thread::current();
                let thread_name = match thread.name() {
                    Some(name) => name,
                    None => panic!("thread doesn't have name"),
                };
                let mut guard = c.lock().expect("lock must succeed");
                let c = guard.entry(thread_name.to_string()).or_default();
                c.set(value)
            }
        }
    }
}

impl Debug for Singleton {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Singleton")
            .field("type", &match self {
                Self::Production(_) => "Production",
                Self::Testing(_) => "Testing",
            })
            .finish()
    }
}

/// GLOBAL is a static type that holding all global data.
static GLOBAL: OnceCell<Singleton> = OnceCell::new();

/// Global is an empty struct that only used to carry associated functions.
pub struct Global;

impl Global {
    /// init production global data registry.
    ///
    /// Should only be initiated once.
    pub fn init_production() {
        GLOBAL
            .set(Singleton::Production(<Container![Send + Sync]>::new()))
            .expect("GLOBAL has been set")
    }

    /// init testing global data registry.
    ///
    /// Should only be initiated once and only used in testing.
    #[cfg(debug_assertions)]
    pub fn init_testing() {
        GLOBAL
            .set(Singleton::Testing(Mutex::default()))
            .expect("GLOBAL has been set")
    }

    /// drop testing global data by thread name.
    ///
    /// Should only be used in testing code.
    #[cfg(debug_assertions)]
    pub fn drop_testing(thread_name: &str) {
        match GLOBAL.wait() {
            Singleton::Production(_) => {
                unreachable!("drop_testing should never be called on production global")
            }
            Singleton::Testing(c) => {
                let mut guard = c.lock().expect("lock must succeed");
                let _ = guard.remove(thread_name);
            }
        }
    }

    /// Get data from global data registry.
    pub fn get<T: Clone + 'static>() -> T {
        GLOBAL.wait().get()
    }

    /// Set data into global data registry.
    pub fn set<T: Send + Sync + 'static>(value: T) {
        let set = GLOBAL.wait().set(value);
        assert!(!set, "value has been set in global data registry");
    }
}
