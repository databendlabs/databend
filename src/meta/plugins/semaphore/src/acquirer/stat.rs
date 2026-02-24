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

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use itertools::EitherOrBoth;
use itertools::Itertools;

use crate::PermitSeq;
use crate::queue::EventDesc;
use crate::queue::PermitEvent;

/// The statistics about running the semaphore acquirer.
#[derive(Debug, Clone)]
pub struct SharedAcquirerStat {
    pub inner: Arc<Mutex<AcquirerStat>>,
}

#[derive(Debug, Clone)]
pub struct AcquirerStat {
    start: Instant,
    /// The timestamps when finished getting a seq from the meta-service
    ///
    /// It might retry several times thus it is a vector.
    get_seq: Vec<Instant>,

    /// The timestamps when insert seq txn finished(successfully or not)
    ///
    /// The last one is the successful one.
    try_insert_seq: Vec<Instant>,

    my_seq: PermitSeq,

    /// The timestamps when the event is received from the subscriber
    ///
    /// This vec may include irrelevant events(with different `sem_seq`).
    receive_event: Vec<(EventDesc, Instant)>,

    /// The timestamp when the permit is acquired, if it is acquired.
    acquired: Option<Instant>,

    /// The timestamp when the permit is removed, if it is removed.
    removed: Option<Instant>,
}

impl fmt::Display for SharedAcquirerStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock().unwrap();

        write!(f, "AcquirerStat{{ latencies: {{ ")?;

        let mut base = inner.start;

        // Display the get-seq/insert-seq CAS loop stat

        write!(
            f,
            "seq(get/insert): {}/{}: [",
            inner.get_seq.len(),
            inner.try_insert_seq.len()
        )?;

        let get_seq_iter = inner.get_seq.iter();
        let try_insert_seq_iter = inner.try_insert_seq.iter();

        let all = get_seq_iter.zip_longest(try_insert_seq_iter).enumerate();

        for (i, x) in all {
            if i > 0 {
                write!(f, ", ")?;
            }
            match x {
                EitherOrBoth::Both(a, b) => {
                    write!(
                        f,
                        "{:.1?}/{:.1?}",
                        a.duration_since(base),
                        b.duration_since(base)
                    )?;
                }
                EitherOrBoth::Left(a) => {
                    write!(f, "{:.1?}/-", a.duration_since(base),)?;
                }
                EitherOrBoth::Right(b) => {
                    write!(f, "-/{:.1?}", b.duration_since(base),)?;
                }
            }
        }

        write!(f, "], ")?;

        if let Some(last) = inner.get_seq.last() {
            base = *last;
        }

        if let Some(last) = inner.try_insert_seq.last() {
            if last > &base {
                base = *last;
            }
        }

        // Display the received events stat

        // When displaying, collect events within a short interval of time in a group.
        // This is the start time of the group.
        let mut window_start = None;
        let mut window_end = None;
        let window_size = Duration::from_millis(5);

        write!(
            f,
            "events(my_seq:{}): {} [",
            inner.my_seq,
            inner.receive_event.len()
        )?;
        for (i, (received, t)) in inner.receive_event.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            if window_start.is_none() {
                window_start = Some(*t);
                write!(f, "(")?;
            }

            window_end = Some(*t);

            if received.seq == inner.my_seq {
                write!(f, "**{received}**",)?;
            } else {
                write!(f, "{received}",)?;
            };

            let start = window_start.unwrap();
            let end = window_end.unwrap();
            if end.duration_since(start) > window_size {
                write!(
                    f,
                    ")=({:.1?}-{:.1?})",
                    start.duration_since(base),
                    end.duration_since(base)
                )?;
                window_start = None;
            }
        }

        if let Some(end) = window_end {
            if let Some(start) = window_start {
                write!(
                    f,
                    ")=({:.1?}-{:.1?})",
                    start.duration_since(base),
                    end.duration_since(base)
                )?;
            }
        }

        write!(f, "], ")?;

        if let Some(acquired) = inner.acquired {
            write!(
                f,
                "total-acquired: {:.1?}, ",
                acquired.duration_since(inner.start)
            )?;
        }

        if let Some(removed) = inner.removed {
            write!(
                f,
                "total-removed: {:.1?}, ",
                removed.duration_since(inner.start)
            )?;
        }

        write!(f, "}} }}")?;

        Ok(())
    }
}

impl Default for SharedAcquirerStat {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedAcquirerStat {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AcquirerStat {
                start: Instant::now(),
                get_seq: vec![],
                try_insert_seq: vec![],
                my_seq: 0,
                receive_event: vec![],
                acquired: None,
                removed: None,
            })),
        }
    }

    pub(crate) fn start(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.start = Instant::now();
    }

    pub(crate) fn on_finish_get_seq(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.get_seq.push(Instant::now());
    }

    pub(crate) fn on_finish_try_insert_seq(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.try_insert_seq.push(Instant::now());
    }

    pub(crate) fn on_insert_seq(&mut self, my_seq: PermitSeq) {
        let mut inner = self.inner.lock().unwrap();
        inner.my_seq = my_seq;
    }

    pub(crate) fn on_receive_event(&mut self, event: &PermitEvent) {
        let mut inner = self.inner.lock().unwrap();
        inner.receive_event.push((event.desc(), Instant::now()));
    }

    pub(crate) fn on_acquire(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.acquired = Some(Instant::now());
    }

    pub(crate) fn on_remove(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.removed = Some(Instant::now());
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> AcquirerStat {
        Arc::try_unwrap(self.inner).unwrap().into_inner().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::PermitEntry;

    #[test]
    fn test_acquirer_stat() {
        let stat = SharedAcquirerStat::new();
        let now = Instant::now();
        let nanos = |nano: u64| now + Duration::from_nanos(nano);

        {
            let mut inner = stat.inner.lock().unwrap();

            inner.start = now;
            inner.get_seq = vec![nanos(100_123456), nanos(200_123456), nanos(250_123456)];
            inner.try_insert_seq = vec![nanos(300_123456), nanos(400_123456)];
            inner.my_seq = 4;
            inner.receive_event = vec![
                (
                    PermitEvent::new_acquired(3, PermitEntry::new("dummy", 0)).desc(),
                    nanos(500_123),
                ),
                (
                    PermitEvent::new_acquired(4, PermitEntry::new("dummy", 0)).desc(),
                    nanos(408_000_123),
                ),
                (
                    PermitEvent::new_removed(5, PermitEntry::new("dummy", 0)).desc(),
                    nanos(600_123456),
                ),
            ];
            inner.acquired = Some(nanos(700_123456));
            inner.removed = Some(nanos(800_123456));
        }

        assert_eq!(
            format!("{}", stat),
            "AcquirerStat{ latencies: { seq(get/insert): 3/2: [100.1ms/300.1ms, 200.1ms/400.1ms, 250.1ms/-], events(my_seq:4): 3 [(A-3, **A-4**)=(0.0ns-7.9ms), (D-5)=(200.0ms-200.0ms)], total-acquired: 700.1ms, total-removed: 800.1ms, } }"
        );
    }
}
