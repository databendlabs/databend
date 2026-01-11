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
use std::sync::PoisonError;

use tokio::sync::watch;

#[derive(Debug)]
struct BarrierState {
    waker: watch::Sender<usize>,
    arrived: usize,
    generation: usize,

    n: usize,
}

pub struct Barrier {
    state: Mutex<BarrierState>,
    wait: watch::Receiver<usize>,
}

impl Barrier {
    pub fn new(mut n: usize) -> Barrier {
        let (waker, wait) = watch::channel(0);

        if n == 0 {
            n = 1;
        }

        Barrier {
            state: Mutex::new(BarrierState {
                n,
                waker,
                arrived: 0,
                generation: 1,
            }),
            wait,
        }
    }

    pub async fn wait(&self) -> BarrierWaitResult {
        let (generation, is_leader) = {
            let locked = self.state.lock();
            let mut state = locked.unwrap_or_else(PoisonError::into_inner);

            let is_leader = state.arrived == 0;
            let generation = state.generation;
            state.arrived += 1;

            if state.arrived == state.n {
                state
                    .waker
                    .send(state.generation)
                    .expect("there is at least one receiver");
                state.arrived = 0;
                state.generation += 1;
                return BarrierWaitResult(is_leader);
            }

            (generation, is_leader)
        };

        let mut wait = self.wait.clone();

        loop {
            let _ = wait.changed().await;

            if *wait.borrow() >= generation {
                break;
            }
        }

        BarrierWaitResult(is_leader)
    }

    pub fn reduce_quorum(&self, n: usize) {
        let locked = self.state.lock();
        let mut state = locked.unwrap_or_else(PoisonError::into_inner);
        state.n -= n;

        if state.arrived >= state.n {
            state
                .waker
                .send(state.generation)
                .expect("there is at least one receiver");
            state.arrived = 0;
            state.generation += 1;
        }
    }
}

#[derive(Debug, Clone)]
pub struct BarrierWaitResult(bool);

impl BarrierWaitResult {
    pub fn is_leader(&self) -> bool {
        self.0
    }
}
