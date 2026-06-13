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

use std::sync::Mutex;

pub trait RuntimeBuilder<T> {
    type Error;
    fn build(&self) -> Result<T, Self::Error>;
}

pub struct Pool<R, B: RuntimeBuilder<R>> {
    pub builder: B,
    runtimes: Mutex<Vec<R>>,
}

impl<R, B: RuntimeBuilder<R>> Pool<R, B> {
    pub fn new(builder: B) -> Self {
        Self {
            builder,
            runtimes: Mutex::new(vec![]),
        }
    }

    pub fn call<T, F, E>(&self, op: F) -> Result<T, E>
    where
        F: FnOnce(&R) -> Result<T, E>,
        E: From<B::Error>,
    {
        let mut runtimes = self.runtimes.lock().unwrap();
        let runtime = match runtimes.pop() {
            Some(runtime) => runtime,
            None => self.builder.build()?,
        };
        drop(runtimes);

        let result = op(&runtime);

        let mut runtimes = self.runtimes.lock().unwrap();
        runtimes.push(runtime);

        result
    }
}
