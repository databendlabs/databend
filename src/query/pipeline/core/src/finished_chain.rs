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
use std::collections::VecDeque;
use std::fmt::Write;
use std::panic::Location;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::catch_unwind;
use databend_common_exception::Result;
use log::info;

use crate::PlanProfile;

pub enum CallbackType {
    // Complete the most basic callbacks for the query. Their invocation must be ensured priority, otherwise it may result in incorrect data.
    Basic,
    // Callbacks that need to be invoked after the pipeline completes a query, such as various hooks
    Normal,
    // Invoked ignore success or failure.
    Always,
}

pub struct ExecutionInfo {
    pub res: Result<()>,
    pub profiling: HashMap<u32, PlanProfile>,
}

impl ExecutionInfo {
    pub fn create(res: Result<()>, profiling: HashMap<u32, PlanProfile>) -> ExecutionInfo {
        ExecutionInfo { res, profiling }
    }
}

pub trait Callback: Send + Sync + 'static {
    fn typ(&self) -> CallbackType {
        CallbackType::Normal
    }

    fn apply(self: Box<Self>, info: &ExecutionInfo) -> Result<()>;
}

struct ApplyState {
    is_always: bool,
    is_interrupt: bool,
    successfully: bool,
    elapsed: Duration,
    location: &'static Location<'static>,
}

pub struct FinishedCallbackChain {
    chain: VecDeque<(&'static Location<'static>, Box<dyn Callback>)>,
}

impl FinishedCallbackChain {
    pub fn create() -> FinishedCallbackChain {
        FinishedCallbackChain {
            chain: VecDeque::new(),
        }
    }

    pub fn push_front(&mut self, location: &'static Location<'static>, f: Box<dyn Callback>) {
        self.chain.push_front((location, f))
    }

    pub fn push_back(&mut self, location: &'static Location<'static>, f: Box<dyn Callback>) {
        self.chain.push_back((location, f))
    }

    pub fn apply(&mut self, mut info: ExecutionInfo) -> Result<()> {
        let chain = std::mem::take(&mut self.chain);

        let mut states = Vec::with_capacity(chain.len());

        let (callbacks, always_callbacks) = {
            let mut basic_callback = vec![];
            let mut normal_callbacks = vec![];
            let mut always_callbacks = vec![];
            for (location, callback) in chain.into_iter() {
                match callback.typ() {
                    CallbackType::Basic => {
                        basic_callback.push((location, callback));
                    }
                    CallbackType::Normal => {
                        normal_callbacks.push((location, callback));
                    }
                    CallbackType::Always => {
                        always_callbacks.push((location, callback));
                    }
                }
            }

            basic_callback.extend(normal_callbacks);
            (basic_callback, always_callbacks)
        };

        let mut apply_res = Ok(());
        for (location, callback) in callbacks {
            if apply_res.is_err() {
                states.push(ApplyState {
                    location,
                    is_always: false,
                    is_interrupt: true,
                    successfully: false,
                    elapsed: Duration::from_secs(0),
                });

                continue;
            }

            let instant = Instant::now();
            if let Err(cause) = callback.apply(&info) {
                states.push(ApplyState {
                    location,
                    is_always: false,
                    is_interrupt: false,
                    successfully: false,
                    elapsed: instant.elapsed(),
                });

                info.res = Err(cause.clone());

                apply_res = Err(cause);
                continue;
            }

            states.push(ApplyState {
                location,
                is_always: false,
                is_interrupt: false,
                successfully: true,
                elapsed: instant.elapsed(),
            });
        }

        Self::apply_always(info, states, always_callbacks);
        apply_res
    }

    fn apply_always(
        info: ExecutionInfo,
        mut states: Vec<ApplyState>,
        always_callbacks: Vec<(&'static Location<'static>, Box<dyn Callback>)>,
    ) {
        for (location, always_callback) in always_callbacks {
            let instant = Instant::now();
            states.push(match always_callback.apply(&info) {
                Ok(_) => ApplyState {
                    location,
                    is_always: true,
                    is_interrupt: false,
                    successfully: true,
                    elapsed: instant.elapsed(),
                },
                Err(_cause) => ApplyState {
                    location,
                    is_always: true,
                    is_interrupt: false,
                    successfully: false,
                    elapsed: instant.elapsed(),
                },
            });
        }

        if !states.is_empty() {
            Self::log_states(&states);
        }
    }

    pub fn extend(&mut self, other: FinishedCallbackChain) {
        self.chain.extend(other.chain)
    }

    fn log_states(apply_states: &[ApplyState]) {
        let mut message = String::new();
        writeln!(&mut message, "Executor apply finished callback state:").unwrap();
        for apply_state in apply_states {
            let execute_state = match apply_state.successfully {
                true => "\u{2705}",
                false => "\u{274C}",
            };

            let always_state = match apply_state.is_always {
                true => "(always) ",
                false => match apply_state.is_interrupt {
                    true => "(interrupt) ",
                    false => "",
                },
            };

            writeln!(
                &mut message,
                "├──{}:{:?} - {}{}:{}:{}",
                execute_state,
                apply_state.elapsed,
                always_state,
                apply_state.location.file(),
                apply_state.location.line(),
                apply_state.location.column()
            )
            .unwrap();
        }

        info!("{}", message);
    }
}

impl<T: FnOnce(&ExecutionInfo) -> Result<()> + Send + Sync + 'static> Callback for T {
    fn apply(self: Box<Self>, info: &ExecutionInfo) -> Result<()> {
        match catch_unwind(move || self(info)) {
            Ok(Ok(_)) => Ok(()),
            Err(cause) => Err(cause),
            Ok(Err(cause)) => Err(cause),
        }
    }
}

pub struct AlwaysCallback<T: Callback> {
    inner: Box<T>,
}

impl<T: Callback> Callback for AlwaysCallback<T> {
    fn typ(&self) -> CallbackType {
        CallbackType::Always
    }

    fn apply(self: Box<Self>, info: &ExecutionInfo) -> Result<()> {
        self.inner.apply(info)
    }
}

pub fn always_callback<T: Callback>(inner: T) -> AlwaysCallback<T> {
    AlwaysCallback {
        inner: Box::new(inner),
    }
}

pub struct BasicCallback<T: Callback> {
    inner: Box<T>,
}

impl<T: Callback> Callback for BasicCallback<T> {
    fn typ(&self) -> CallbackType {
        CallbackType::Basic
    }

    fn apply(self: Box<Self>, info: &ExecutionInfo) -> Result<()> {
        self.inner.apply(info)
    }
}

pub fn basic_callback<T: Callback>(inner: T) -> BasicCallback<T> {
    BasicCallback {
        inner: Box::new(inner),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::panic::Location;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;

    use crate::always_callback;
    use crate::basic_callback;
    use crate::ExecutionInfo;
    use crate::FinishedCallbackChain;

    #[test]
    fn test_callback_order() -> Result<()> {
        let mut chain = FinishedCallbackChain::create();

        let seq = Arc::new(AtomicUsize::new(0));

        for index in 0..10 {
            chain.push_back(
                Location::caller(),
                Box::new({
                    let seq = seq.clone();
                    move |_info: &ExecutionInfo| {
                        let seq = seq.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(index, seq);
                        Ok(())
                    }
                }),
            );
        }

        chain.apply(ExecutionInfo::create(Ok(()), HashMap::new()))?;

        assert_eq!(seq.load(Ordering::SeqCst), 10);

        Ok(())
    }

    #[test]
    fn test_callback_order_with_basic_callback() -> Result<()> {
        let mut chain = FinishedCallbackChain::create();

        let seq = Arc::new(AtomicUsize::new(0));

        for index in 0..10 {
            chain.push_back(
                Location::caller(),
                Box::new({
                    let seq = seq.clone();
                    move |_info: &ExecutionInfo| {
                        let seq = seq.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(3 + index, seq);
                        Ok(())
                    }
                }),
            );
        }

        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                basic_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(1, seq);
                    Ok(())
                })
            }),
        );

        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                basic_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(0, seq);
                    Ok(())
                })
            }),
        );

        // always callback after all callback
        chain.push_back(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                basic_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(2, seq);
                    Ok(())
                })
            }),
        );

        chain.apply(ExecutionInfo::create(Ok(()), HashMap::new()))?;

        assert_eq!(seq.load(Ordering::SeqCst), 13);

        Ok(())
    }

    #[test]
    fn test_callback_order_with_always_callback() -> Result<()> {
        let mut chain = FinishedCallbackChain::create();

        let seq = Arc::new(AtomicUsize::new(0));

        for index in 0..10 {
            chain.push_back(
                Location::caller(),
                Box::new({
                    let seq = seq.clone();
                    move |_info: &ExecutionInfo| {
                        let seq = seq.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(index, seq);
                        Ok(())
                    }
                }),
            );
        }

        // always callback after all callback
        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(11, seq);
                    Ok(())
                })
            }),
        );

        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(10, seq);
                    Ok(())
                })
            }),
        );

        // always callback after all callback
        chain.push_back(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(12, seq);
                    Ok(())
                })
            }),
        );

        chain.apply(ExecutionInfo::create(Ok(()), HashMap::new()))?;

        assert_eq!(seq.load(Ordering::SeqCst), 13);

        Ok(())
    }

    #[test]
    fn test_always_callback() -> Result<()> {
        let mut chain = FinishedCallbackChain::create();

        let seq = Arc::new(AtomicUsize::new(0));

        for index in 0..10 {
            chain.push_back(
                Location::caller(),
                Box::new({
                    let seq = seq.clone();
                    move |_info: &ExecutionInfo| {
                        let seq = seq.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(index, seq);
                        Ok(())
                    }
                }),
            );
        }

        chain.push_back(
            Location::caller(),
            Box::new(|_info: &ExecutionInfo| Err(ErrorCode::Internal(""))),
        );

        for _index in 0..10 {
            chain.push_back(
                Location::caller(),
                Box::new(|_info: &ExecutionInfo| unreachable!("unreachable")),
            );
        }

        // always callback after all callback
        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(11, seq);
                    Ok(())
                })
            }),
        );

        chain.push_front(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(10, seq);
                    Ok(())
                })
            }),
        );

        // always callback after all callback
        chain.push_back(
            Location::caller(),
            Box::new({
                let seq = seq.clone();
                always_callback(move |_info: &ExecutionInfo| {
                    let seq = seq.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(12, seq);
                    Ok(())
                })
            }),
        );

        assert!(
            chain
                .apply(ExecutionInfo::create(Ok(()), HashMap::new()))
                .is_err()
        );

        assert_eq!(seq.load(Ordering::SeqCst), 13);

        Ok(())
    }
}
