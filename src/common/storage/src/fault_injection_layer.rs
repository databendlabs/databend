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

//! Storage fault injection for tests.
//!
//! Storage errors are the least tested branch of every storage operation: object stores
//! rarely fail in a test run, so the code that handles `NotFound` on a snapshot, a failed
//! segment write during commit, or a delete that did not happen during vacuum only runs in
//! production. Bugs of the form "error swallowed and reported as success" or "raw storage
//! error surfaced instead of a domain error" have been found by users more than once.
//!
//! [`FaultInjectionLayer`] sits below the retry layer of every operator built by
//! [`crate::init_operator`]. It is inert unless a test installs a [`FaultRule`] through
//! [`FaultInjection`]: the fast path is one relaxed atomic load per operation. A rule
//! matches an operation kind and a path substring, injects one of the [`FaultKind`]s, and
//! optionally expires after a number of hits; the [`FaultHandle`] returned on install
//! reports how often the rule fired, so a test can assert the fault was actually exercised.
//!
//! ```ignore
//! let fault = FaultInjection::install(FaultRule::new(FaultOp::Write, "_ss/", FaultKind::Permanent));
//! assert!(insert(...).await.is_err());
//! assert_eq!(fault.hits(), 1);
//! fault.remove();
//! ```

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use opendal::Error;
use opendal::ErrorKind;
use opendal::Result;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpRead;
use opendal::raw::OpStat;
use opendal::raw::OpWrite;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpStat;
use opendal::raw::RpWrite;
use opendal::raw::oio;

/// The storage operation a rule applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    Read,
    Write,
    Stat,
    List,
    Delete,
}

/// The error injected when a rule fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// `ErrorKind::NotFound`: the object does not exist.
    NotFound,
    /// `ErrorKind::Unexpected`, not marked temporary: the retry layer gives up immediately.
    Permanent,
    /// `ErrorKind::Unexpected`, marked temporary: the retry layer retries it.
    Temporary,
}

impl FaultKind {
    fn error(self, op: FaultOp, path: &str) -> Error {
        let message = format!("injected {op:?} fault on `{path}`");
        match self {
            FaultKind::NotFound => Error::new(ErrorKind::NotFound, message),
            FaultKind::Permanent => Error::new(ErrorKind::Unexpected, message),
            FaultKind::Temporary => Error::new(ErrorKind::Unexpected, message).set_temporary(),
        }
    }
}

/// A fault to inject: `kind` for every `op` whose path contains `path_contains`, at most
/// `remaining` times (unlimited when `None`).
#[derive(Debug, Clone)]
pub struct FaultRule {
    pub op: FaultOp,
    pub path_contains: String,
    pub kind: FaultKind,
    pub remaining: Option<usize>,
}

impl FaultRule {
    pub fn new(op: FaultOp, path_contains: impl Into<String>, kind: FaultKind) -> Self {
        Self {
            op,
            path_contains: path_contains.into(),
            kind,
            remaining: None,
        }
    }

    /// Fire at most `times` times, then stop matching.
    pub fn times(mut self, times: usize) -> Self {
        self.remaining = Some(times);
        self
    }
}

struct RuleState {
    rule: FaultRule,
    remaining: Option<AtomicUsize>,
    hits: AtomicUsize,
}

impl RuleState {
    /// Returns the error to inject if this rule matches and has budget left.
    fn fire(&self, op: FaultOp, path: &str) -> Option<Error> {
        if self.rule.op != op || !path.contains(&self.rule.path_contains) {
            return None;
        }
        if let Some(remaining) = &self.remaining {
            // Claim one unit of budget; back off if it is exhausted.
            let mut current = remaining.load(Ordering::Acquire);
            loop {
                if current == 0 {
                    return None;
                }
                match remaining.compare_exchange_weak(
                    current,
                    current - 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(self.rule.kind.error(op, path))
    }
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static RULES: LazyLock<Mutex<Vec<Arc<RuleState>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Global registry of installed fault rules.
pub struct FaultInjection;

impl FaultInjection {
    /// Install a rule; it applies to every operator until removed.
    pub fn install(rule: FaultRule) -> FaultHandle {
        let state = Arc::new(RuleState {
            remaining: rule.remaining.map(AtomicUsize::new),
            hits: AtomicUsize::new(0),
            rule,
        });
        let mut rules = RULES.lock().unwrap();
        rules.push(state.clone());
        ENABLED.store(true, Ordering::Release);
        FaultHandle { state }
    }

    /// Remove every installed rule.
    pub fn clear() {
        let mut rules = RULES.lock().unwrap();
        rules.clear();
        ENABLED.store(false, Ordering::Release);
    }

    fn check(op: FaultOp, path: &str) -> Result<()> {
        if !ENABLED.load(Ordering::Relaxed) {
            return Ok(());
        }
        let rules = RULES.lock().unwrap();
        for rule in rules.iter() {
            if let Some(err) = rule.fire(op, path) {
                return Err(err);
            }
        }
        Ok(())
    }
}

/// Handle to an installed rule: observe how often it fired, remove it.
pub struct FaultHandle {
    state: Arc<RuleState>,
}

impl FaultHandle {
    /// How many operations this rule turned into errors so far.
    pub fn hits(&self) -> usize {
        self.state.hits.load(Ordering::Relaxed)
    }

    /// Uninstall this rule only.
    pub fn remove(self) {
        let mut rules = RULES.lock().unwrap();
        rules.retain(|r| !Arc::ptr_eq(r, &self.state));
        if rules.is_empty() {
            ENABLED.store(false, Ordering::Release);
        }
    }
}

/// See the module documentation.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultInjectionLayer;

impl<A: Access> Layer<A> for FaultInjectionLayer {
    type LayeredAccess = FaultInjectionAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        FaultInjectionAccessor { inner }
    }
}

pub struct FaultInjectionAccessor<A> {
    inner: A,
}

impl<A> Debug for FaultInjectionAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultInjectionAccessor").finish()
    }
}

impl<A: Access> LayeredAccess for FaultInjectionAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = FaultInjectionDeleter<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        FaultInjection::check(FaultOp::Read, path)?;
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        FaultInjection::check(FaultOp::Write, path)?;
        self.inner.write(path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        FaultInjection::check(FaultOp::Stat, path)?;
        self.inner.stat(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        FaultInjection::check(FaultOp::List, path)?;
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, deleter) = self.inner.delete().await?;
        Ok((rp, FaultInjectionDeleter { inner: deleter }))
    }
}

/// Deletes are batched: the path is only known when it is queued, so that is where the
/// fault fires.
pub struct FaultInjectionDeleter<D> {
    inner: D,
}

impl<D: oio::Delete> oio::Delete for FaultInjectionDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        FaultInjection::check(FaultOp::Delete, path)?;
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod tests {
    use opendal::Operator;
    use opendal::services::Memory;

    use super::*;

    // The rule registry is global; run these tests one at a time.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn operator() -> Operator {
        Operator::new(Memory::default())
            .unwrap()
            .layer(FaultInjectionLayer)
            .finish()
    }

    #[tokio::test]
    async fn inert_without_rules() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("a/b", "x").await?;
        assert_eq!(op.read("a/b").await?.to_vec(), b"x");
        Ok(())
    }

    #[tokio::test]
    async fn matches_op_and_path_and_counts_hits() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("_ss/1", "x").await?;
        op.write("_b/1", "y").await?;

        let fault =
            FaultInjection::install(FaultRule::new(FaultOp::Read, "_ss/", FaultKind::NotFound));
        let err = op.read("_ss/1").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(
            op.read("_b/1").await?.to_vec(),
            b"y",
            "other paths unaffected"
        );
        assert!(op.write("_ss/2", "z").await.is_ok(), "other ops unaffected");
        assert_eq!(fault.hits(), 1);

        fault.remove();
        assert_eq!(op.read("_ss/1").await?.to_vec(), b"x");
        Ok(())
    }

    #[tokio::test]
    async fn expires_after_the_given_number_of_hits() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("k", "v").await?;
        let fault = FaultInjection::install(
            FaultRule::new(FaultOp::Read, "k", FaultKind::Temporary).times(2),
        );
        assert!(op.read("k").await.unwrap_err().is_temporary());
        assert!(op.read("k").await.is_err());
        assert_eq!(op.read("k").await?.to_vec(), b"v");
        assert_eq!(fault.hits(), 2);
        fault.remove();
        Ok(())
    }

    #[tokio::test]
    async fn delete_fault_fires_per_path() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("_sg/1", "x").await?;
        op.write("_b/1", "y").await?;
        let fault = FaultInjection::install(FaultRule::new(
            FaultOp::Delete,
            "_sg/",
            FaultKind::Permanent,
        ));
        assert!(op.delete("_sg/1").await.is_err());
        op.delete("_b/1").await?;
        assert!(
            op.exists("_sg/1").await?,
            "failed delete must not remove the object"
        );
        assert!(!op.exists("_b/1").await?);
        assert_eq!(fault.hits(), 1);
        fault.remove();
        Ok(())
    }
}
