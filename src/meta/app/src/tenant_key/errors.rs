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
use std::fmt::Formatter;

use databend_meta_types::MatchSeq;

use crate::app_error::AppErrorMessage;
use crate::tenant_key::resource::TenantResource;

/// Error occurred when a record already exists for a key.
#[derive(thiserror::Error)]
pub struct ExistError<R, N = String> {
    name: N,
    ctx: String,
    _p: std::marker::PhantomData<R>,
}

impl<R, N> Clone for ExistError<R, N>
where N: Clone
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            ctx: self.ctx.clone(),
            _p: Default::default(),
        }
    }
}

impl<R, N> PartialEq for ExistError<R, N>
where N: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.ctx == other.ctx
    }
}
impl<R, N> Eq for ExistError<R, N> where N: PartialEq {}

impl<R, N> ExistError<R, N> {
    pub fn new(name: N, ctx: impl ToString) -> Self {
        Self {
            name,
            ctx: ctx.to_string(),
            _p: Default::default(),
        }
    }

    pub fn name(&self) -> &N {
        &self.name
    }

    pub fn ctx(&self) -> &str {
        &self.ctx
    }
}

impl<R, N> fmt::Debug for ExistError<R, N>
where
    R: TenantResource,
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let typ = type_name::<R>();

        f.debug_struct("ExistError")
            .field("type", &typ)
            .field("name", &self.name)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<R, N> fmt::Display for ExistError<R, N>
where
    R: TenantResource,
    N: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = type_name::<R>();
        write!(f, "{typ} '{}' already exists: {}", self.name, self.ctx)
    }
}

impl<R, N> AppErrorMessage for ExistError<R, N>
where
    R: TenantResource,
    N: fmt::Display,
{
}

/// Error occurred when a record not found for a key.
#[derive(thiserror::Error)]
pub struct UnknownError<R, N = String> {
    name: N,
    match_seq: MatchSeq,
    ctx: String,
    _p: std::marker::PhantomData<R>,
}

impl<R, N> Clone for UnknownError<R, N>
where N: Clone
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            match_seq: self.match_seq,
            ctx: self.ctx.clone(),
            _p: Default::default(),
        }
    }
}

impl<R, N> PartialEq for UnknownError<R, N>
where N: PartialEq
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.match_seq == other.match_seq && self.ctx == other.ctx
    }
}

impl<R, N> Eq for UnknownError<R, N> where N: PartialEq {}

impl<R, N> UnknownError<R, N> {
    pub fn new_match_seq(name: N, match_seq: MatchSeq, ctx: impl ToString) -> Self {
        Self {
            name,
            match_seq,
            ctx: ctx.to_string(),
            _p: Default::default(),
        }
    }

    pub fn new(name: N, ctx: impl ToString) -> Self {
        Self {
            name,
            match_seq: MatchSeq::GE(0),
            ctx: ctx.to_string(),
            _p: Default::default(),
        }
    }

    pub fn name(&self) -> &N {
        &self.name
    }

    pub fn ctx(&self) -> &str {
        &self.ctx
    }
}

impl<R, N> fmt::Debug for UnknownError<R, N>
where
    R: TenantResource,
    N: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let typ = type_name::<R>();

        f.debug_struct("UnknownError")
            .field("type", &typ)
            .field("name", &self.name)
            .field("match_seq", &self.match_seq)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<R, N> fmt::Display for UnknownError<R, N>
where
    R: TenantResource,
    N: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let typ = type_name::<R>();
        write!(
            f,
            "Unknown {typ} '{}'(seq {}): {}",
            self.name, self.match_seq, self.ctx
        )
    }
}

impl<R, N> AppErrorMessage for UnknownError<R, N>
where
    R: TenantResource,
    N: fmt::Display,
{
}

fn type_name<R: TenantResource>() -> &'static str {
    let full_name = std::any::type_name::<R::ValueType>();
    let mut segs = full_name.rsplitn(2, "::");
    segs.next().unwrap()
}

#[cfg(test)]
mod tests {
    use databend_meta_types::MatchSeq;

    use crate::principal::network_policy_ident;

    #[test]
    fn test_exist_error() {
        let err = super::ExistError::<network_policy_ident::Resource> {
            name: "foo".to_string(),
            ctx: "bar".to_string(),
            _p: Default::default(),
        };

        let got = err.to_string();
        assert_eq!(got, "NetworkPolicy 'foo' already exists: bar")
    }

    #[test]
    fn test_unknown_error() {
        let err = super::UnknownError::<network_policy_ident::Resource> {
            name: "foo".to_string(),
            match_seq: MatchSeq::GE(1),
            ctx: "bar".to_string(),
            _p: Default::default(),
        };

        let got = err.to_string();
        assert_eq!(got, "Unknown NetworkPolicy 'foo'(seq >= 1): bar")
    }
}
