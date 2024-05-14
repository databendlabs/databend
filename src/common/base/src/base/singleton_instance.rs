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

use std::fmt::Debug;

use once_cell::sync::OnceCell;
use state::Container;

/// SINGLETON_TYPE is the global singleton type.
static SINGLETON_TYPE: OnceCell<SingletonType> = OnceCell::new();

/// GLOBAL is a static type that holding all global data.
static GLOBAL: OnceCell<Container![Send + Sync]> = OnceCell::new();

#[cfg(debug_assertions)]
/// LOCAL is a static type that holding all global data only for local tests.
static LOCAL: OnceCell<
    parking_lot::RwLock<std::collections::HashMap<String, Container![Send + Sync]>>,
> = OnceCell::new();

/// Singleton is a wrapper enum for `Container![Send + Sync]`.
///
/// - `Production` is used in our production code.
/// - `Testing` is served for test only and gated under `debug_assertions`.
pub enum SingletonType {
    Production,

    #[cfg(debug_assertions)]
    Testing,
}

impl SingletonType {
    fn get<T: Clone + 'static>(&self) -> T {
        match self {
            SingletonType::Production => {
                let v: &T = GLOBAL.wait().get();
                v.clone()
            }
            #[cfg(debug_assertions)]
            SingletonType::Testing => {
                let thread_name = std::thread::current()
                    .name()
                    .expect("thread doesn't have name")
                    .to_string();
                let guard = LOCAL.wait().read();
                let v: &T = guard
                    .get(&thread_name)
                    .unwrap_or_else(|| panic!("thread {thread_name} is not initiated. If test is passed, please ignore this panic, it's expected. Otherwise, test must have been set up incorrectly, please check your test code again."))
                    .get();
                v.clone()
            }
        }
    }

    fn set<T: Send + Sync + 'static>(&self, value: T) -> bool {
        match self {
            SingletonType::Production => GLOBAL.wait().set(value),
            #[cfg(debug_assertions)]
            SingletonType::Testing => {
                let thread_name = std::thread::current()
                    .name()
                    .expect("thread doesn't have name")
                    .to_string();
                let guard = LOCAL.wait().read();
                guard
                    .get(&thread_name)
                    .unwrap_or_else(|| panic!("thread {thread_name} is not initiated. If test is passed, please ignore this panic, it's expected. Otherwise, test must have been set up incorrectly, please check your test code again."))
                    .set(value)
            }
        }
    }
}

impl Debug for SingletonType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Singleton")
            .field("type", &match self {
                Self::Production => "Production",
                #[cfg(debug_assertions)]
                Self::Testing => "Testing",
            })
            .finish()
    }
}

/// Global is an empty struct that only used to carry associated functions.
pub struct GlobalInstance;

impl GlobalInstance {
    /// init production global data registry.
    ///
    /// Should only be initiated once.
    pub fn init_production() {
        let _ = SINGLETON_TYPE.set(SingletonType::Production);
        let _ = GLOBAL.set(<Container![Send + Sync]>::new());
    }

    /// init testing global data registry.
    ///
    /// Should only be initiated once and only used in testing.
    #[cfg(debug_assertions)]
    pub fn init_testing(thread_name: &str) {
        let _ = SINGLETON_TYPE.set(SingletonType::Testing);
        let _ = LOCAL.set(parking_lot::RwLock::default());

        let v = LOCAL
            .wait()
            .write()
            .insert(thread_name.to_string(), <Container![Send + Sync]>::new());
        assert!(
            v.is_none(),
            "thread {thread_name} has been initiated before"
        )
    }

    /// drop testing global data by thread name.
    ///
    /// Should only be used in testing code.
    #[cfg(debug_assertions)]
    pub fn drop_testing(thread_name: &str) {
        // Make sure the write lock is released before calling drop.
        let _ = { LOCAL.wait().write().remove(thread_name) };
    }

    /// Get data from global data registry.
    pub fn get<T: Clone + 'static>() -> T {
        SINGLETON_TYPE
            .get()
            .expect("global data registry must be initiated")
            .get()
    }

    pub fn try_get<T: Clone + 'static>() -> Option<T> {
        SINGLETON_TYPE.get().map(|v| v.get())
    }

    /// Set data into global data registry.
    pub fn set<T: Send + Sync + 'static>(value: T) {
        let set = SINGLETON_TYPE
            .get()
            .expect("global data registry must be initiated")
            .set(value);
        assert!(set, "value has been set in global data registry");
    }
}
