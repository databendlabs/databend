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

/// Implements TryFrom<RelOperator> for a specific type or multiple types.
/// This macro reduces boilerplate code for downcasting RelOperator variants.
///
/// # Examples
///
/// For a single type:
/// ```rust
/// impl_try_from_rel_operator!(MutationSource);
/// ```
///
/// For multiple types:
/// ```rust
/// impl_try_from_rel_operator! {
///     Aggregate,
///     Filter,
///     Join
/// }
/// ```
///
/// This expands to:
///
/// ```rust
/// impl TryFrom<RelOperator> for MutationSource {
///     type Error = ErrorCode;
///     fn try_from(value: RelOperator) -> Result<Self> {
///         if let RelOperator::MutationSource(value) = value {
///             Ok(value)
///         } else {
///             Err(ErrorCode::Internal(format!(
///                 "Cannot downcast {:?} to MutationSource",
///                 value.rel_op()
///             )))
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_try_from_rel_operator {
    // For a single type
    ($ty:ident) => {
        impl TryFrom<RelOperator> for $ty {
            type Error = ErrorCode;
            fn try_from(value: RelOperator) -> Result<Self> {
                if let RelOperator::$ty(value) = value {
                    Ok(value)
                } else {
                    Err(ErrorCode::Internal(format!(
                        "Cannot downcast {:?} to {}",
                        value.rel_op(),
                        stringify!($ty)
                    )))
                }
            }
        }
        impl From<$ty> for RelOperator {
            fn from(value: $ty) -> Self {
                RelOperator::$ty(value)
            }
        }

        impl From<$ty> for Arc<RelOperator> {
            fn from(value: $ty) -> Self {
                Arc::new(RelOperator::$ty(value))
            }
        }
    };
    // Multiple types implementation
    ($($ty:ident),+) => {
        $(impl_try_from_rel_operator!($ty);)+
    };

}

#[macro_export]
macro_rules! impl_match_rel_op {
    ($self:ident, $rel_op:ident, $method:ident, $($arg:expr),*) => {
        match $self {
            RelOperator::Scan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Join($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::EvalScalar($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Filter($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::SecureFilter($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Aggregate($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Sort($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Limit($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Exchange($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::UnionAll($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::DummyTableScan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Window($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::ProjectSet($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::ConstantTableScan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::ExpressionScan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::CacheScan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Udf($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::RecursiveCteScan($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::AsyncFunction($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Mutation($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::CompactBlock($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::MutationSource($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::MaterializedCTE($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::MaterializedCTERef($rel_op) => $rel_op.$method($($arg),*),
            RelOperator::Sequence($rel_op) => $rel_op.$method($($arg),*),
        }
    }
}

#[macro_export]
macro_rules! match_rel_op {
    ($self:ident, $method:ident) => { impl_match_rel_op!($self, rel_op, $method,) };
    ($self:ident, $method:ident($($arg:expr),*)) => { impl_match_rel_op!($self, rel_op, $method, $($arg),*) };
}
