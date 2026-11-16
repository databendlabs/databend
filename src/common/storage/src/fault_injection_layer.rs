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
//! A rule is process-global, so it must be scoped to the fixture that installed it: other
//! tests in the same binary keep doing storage IO while it is installed, and they must not see
//! it. The path is matched as a prefix, and [`FaultRule::in_root`] binds the rule to one
//! storage root, since fixtures with separate roots and metastores can share a relative table
//! prefix. The rule is removed when its [`FaultHandle`] is dropped, so a failing test cannot
//! leave it behind for the rest of the process.
//!
//! ```ignore
//! let prefix = table_prefix(&fixture);
//! let fault = FaultInjection::install(
//!     FaultRule::new(FaultOp::Write, format!("{prefix}/_ss/"), FaultKind::Permanent)
//!         .in_root(fixture.storage_root()),
//! );
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
use opendal::raw::normalize_root;
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

/// A fault to inject: `kind` for every `op` on a path under `path_contains`, at most
/// `remaining` times (unlimited when `None`).
///
/// `path_contains` is matched as a prefix, so a rule is anchored to the storage prefix of the
/// fixture that installed it. A pattern like `_ss/` does not match another fixture's
/// `1/11/_ss/…`, which a substring match would.
///
/// The path an operator reports is relative to its root, and fixtures with separate roots and
/// separate metastores can hand out the same relative table prefix. `root`, when set,
/// restricts the rule to operators with that storage root; see [`FaultRule::in_root`].
#[derive(Debug, Clone)]
pub struct FaultRule {
    pub op: FaultOp,
    pub path_contains: String,
    pub kind: FaultKind,
    pub remaining: Option<usize>,
    /// Normalized as opendal does (`/a/b/`), compared with the operator's reported root.
    pub root: Option<String>,
}

impl FaultRule {
    pub fn new(op: FaultOp, path_contains: impl Into<String>, kind: FaultKind) -> Self {
        Self {
            op,
            path_contains: path_contains.into(),
            kind,
            remaining: None,
            root: None,
        }
    }

    /// Fire at most `times` times, then stop matching.
    pub fn times(mut self, times: usize) -> Self {
        self.remaining = Some(times);
        self
    }

    /// Fire only for operators whose storage root is `root` (a fixture's `storage_root()`).
    /// Without it, a rule reaches every operator in the process whose relative paths match.
    pub fn in_root(mut self, root: impl AsRef<str>) -> Self {
        self.root = Some(normalize_root(root.as_ref()));
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
    fn fire(&self, op: FaultOp, root: &str, path: &str) -> Option<Error> {
        if self.rule.op != op || !path.starts_with(&self.rule.path_contains) {
            return None;
        }
        if self.rule.root.as_deref().is_some_and(|r| r != root) {
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
    /// Install a rule; it applies to every operator that touches a matching path until the
    /// returned handle is dropped (or [`FaultHandle::remove`] is called).
    pub fn install(rule: FaultRule) -> FaultHandle {
        // An empty pattern matches every path, which would also fail unrelated tests that run
        // storage IO in the same process. Rules are only useful when scoped.
        assert!(
            !rule.path_contains.is_empty(),
            "a fault rule must be scoped to a path such as the fixture's table prefix"
        );
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

    fn check(op: FaultOp, root: &str, path: &str) -> Result<()> {
        if !ENABLED.load(Ordering::Relaxed) {
            return Ok(());
        }
        let rules = RULES.lock().unwrap();
        for rule in rules.iter() {
            if let Some(err) = rule.fire(op, root, path) {
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
        // Dropping the handle uninstalls as well; this only makes it explicit.
    }
}

impl Drop for FaultHandle {
    /// A test that panics must not leave its rule installed for the rest of the process: the
    /// registry is global and every later test would run against it.
    fn drop(&mut self) {
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
        // The storage root of the operator this layer sits in; rules can be restricted to it.
        // Services differ in whether they report it with a trailing slash, so normalize it
        // the same way `FaultRule::in_root` does.
        let root = normalize_root(&inner.info().root()).into();
        FaultInjectionAccessor { inner, root }
    }
}

pub struct FaultInjectionAccessor<A> {
    inner: A,
    root: Arc<str>,
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
        FaultInjection::check(FaultOp::Read, &self.root, path)?;
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        FaultInjection::check(FaultOp::Write, &self.root, path)?;
        self.inner.write(path, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        FaultInjection::check(FaultOp::Stat, &self.root, path)?;
        self.inner.stat(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        FaultInjection::check(FaultOp::List, &self.root, path)?;
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let (rp, deleter) = self.inner.delete().await?;
        Ok((rp, FaultInjectionDeleter {
            inner: deleter,
            root: self.root.clone(),
        }))
    }
}

/// Deletes are batched: the path is only known when it is queued, so that is where the
/// fault fires.
pub struct FaultInjectionDeleter<D> {
    inner: D,
    root: Arc<str>,
}

impl<D: oio::Delete> oio::Delete for FaultInjectionDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        FaultInjection::check(FaultOp::Delete, &self.root, path)?;
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
        operator_in("/")
    }

    fn operator_in(root: &str) -> Operator {
        Operator::new(Memory::default().root(root))
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
    async fn rule_scoped_to_one_prefix_does_not_fire_for_another() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("1/11/_ss/a", "x").await?;
        op.write("2/22/_ss/b", "y").await?;

        // The reported failure: a rule that only names the snapshot directory, not the table
        // prefix, used to match every table's snapshot write in the process.
        let fault =
            FaultInjection::install(FaultRule::new(FaultOp::Read, "_ss/", FaultKind::NotFound));
        assert_eq!(
            op.read("2/22/_ss/b").await?.to_vec(),
            b"y",
            "a rule must not reach another prefix"
        );
        assert_eq!(fault.hits(), 0);

        let fault = FaultInjection::install(FaultRule::new(
            FaultOp::Read,
            "1/11/_ss/",
            FaultKind::NotFound,
        ));
        assert!(op.read("1/11/_ss/a").await.is_err());
        assert_eq!(
            op.read("2/22/_ss/b").await?.to_vec(),
            b"y",
            "other prefixes stay untouched"
        );
        assert_eq!(fault.hits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_handle_uninstalls_the_rule() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        let op = operator();
        op.write("_ss/1", "x").await?;
        {
            let _fault =
                FaultInjection::install(FaultRule::new(FaultOp::Read, "_ss/", FaultKind::NotFound));
            assert!(op.read("_ss/1").await.is_err());
        }
        assert_eq!(
            op.read("_ss/1").await?.to_vec(),
            b"x",
            "a rule must not outlive its handle"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rule_bound_to_a_root_does_not_fire_for_another_root() -> Result<()> {
        let _serial = SERIAL.lock().await;
        FaultInjection::clear();
        // Two fixtures with their own storage roots and metastores hand out the same relative
        // table prefix; a rule installed by one must not fail the other's IO.
        let op_a = operator_in("/fixture-a");
        let op_b = operator_in("/fixture-b");
        op_a.write("1/11/_ss/a", "x").await?;
        op_b.write("1/11/_ss/b", "y").await?;

        let fault = FaultInjection::install(
            FaultRule::new(FaultOp::Read, "1/11/_ss/", FaultKind::NotFound).in_root("/fixture-a"),
        );
        assert!(op_a.read("1/11/_ss/a").await.is_err());
        assert_eq!(
            op_b.read("1/11/_ss/b").await?.to_vec(),
            b"y",
            "the same relative path under another root stays untouched"
        );
        assert_eq!(fault.hits(), 1);
        drop(fault);

        // The root is compared in its normalized form.
        let fault = FaultInjection::install(
            FaultRule::new(FaultOp::Read, "1/11/_ss/", FaultKind::NotFound).in_root("fixture-b/"),
        );
        assert!(op_b.read("1/11/_ss/b").await.is_err());
        assert_eq!(op_a.read("1/11/_ss/a").await?.to_vec(), b"x");
        assert_eq!(fault.hits(), 1);
        Ok(())
    }

    // No `clear()` here: the empty pattern is rejected before the registry is touched, and
    // clearing it without `SERIAL` would erase rules of the tests running next to this one.
    #[test]
    #[should_panic(expected = "a fault rule must be scoped to a path")]
    fn unscoped_rule_is_rejected() {
        let _fault =
            FaultInjection::install(FaultRule::new(FaultOp::Write, "", FaultKind::Permanent));
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
