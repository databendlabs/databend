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

use std::fmt::Debug;

use once_cell::sync::OnceCell;
use state::Container;

/// Singleton is a wrapper enum for `Container![Send + Sync]`.
///
/// - `Production` is used in our production code.
/// - `Testing` is served for test only and gated under `debug_assertions`.
pub enum Singleton {
    Production(Container![Send + Sync]),

    #[cfg(debug_assertions)]
    Testing(parking_lot::Mutex<std::collections::HashMap<String, Container![Send + Sync]>>),
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
                let guard = c.lock();
                let v: &T = guard
                    .get(thread_name)
                    .unwrap_or_else(|| panic!("thread {thread_name} is not initiated"))
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
                let mut guard = c.lock();
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
                #[cfg(debug_assertions)]
                Self::Testing(_) => "Testing",
            })
            .finish()
    }
}

/// GLOBAL is a static type that holding all global data.
static GLOBAL: OnceCell<Singleton> = OnceCell::new();

/// Global is an empty struct that only used to carry associated functions.
pub struct GlobalInstance;

impl GlobalInstance {
    /// init production global data registry.
    ///
    /// Should only be initiated once.
    pub fn init_production() {
        let _ = GLOBAL.set(Singleton::Production(<Container![Send + Sync]>::new()));
    }

    /// init testing global data registry.
    ///
    /// Should only be initiated once and only used in testing.
    #[cfg(debug_assertions)]
    pub fn init_testing() {
        let _ = GLOBAL.set(Singleton::Testing(parking_lot::Mutex::default()));
    }

    /// drop testing global data by thread name.
    ///
    /// Should only be used in testing code.
    #[cfg(debug_assertions)]
    pub fn drop_testing(thread_name: &str) {
        use std::thread;

        match GLOBAL.wait() {
            Singleton::Production(_) => {
                unreachable!("drop_testing should never be called on production global")
            }
            Singleton::Testing(c) => {
                // let v = {
                //     let mut guard = c.lock();
                //     guard.remove(thread_name)
                // };
                // // We don't care about if about this container any more, just
                // // move to another thread to make sure this call returned ASAP.
                // thread::spawn(move || v);
            }
        }
    }

    /// Get data from global data registry.
    pub fn get<T: Clone + 'static>() -> T {
        GLOBAL
            .get()
            .expect("global data registry must be initiated")
            .get()
    }

    /// Set data into global data registry.
    pub fn set<T: Send + Sync + 'static>(value: T) {
        let set = GLOBAL
            .get()
            .expect("global data registry must be initiated")
            .set(value);
        assert!(set, "value has been set in global data registry");
    }
}
