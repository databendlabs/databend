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
    (@enum $operator:ident, $variant:ident, $payload:ident) => {
        impl TryFrom<$operator> for $payload {
            type Error = ErrorCode;
            fn try_from(value: $operator) -> Result<Self> {
                if let $operator::$variant(value) = value { Ok(value) }
                else { Err(ErrorCode::Internal(format!("Cannot downcast {:?} to {}", value.rel_op(), stringify!($payload)))) }
            }
        }
        impl From<$payload> for $operator {
            fn from(value: $payload) -> Self { Self::$variant(value) }
        }
        impl From<$payload> for Arc<$operator> {
            fn from(value: $payload) -> Self { Arc::new($operator::$variant(value)) }
        }
    };
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
        $crate::impl_match_rel_op!(@enum RelOperator, $self, $rel_op, $method, $($arg),*)
    };
    (@enum PhysicalRelOperator, $self:ident, $rel_op:ident, $method:ident, $($arg:expr),*) => {
        $crate::impl_match_rel_op!(@dispatch PhysicalRelOperator, $self, $rel_op, $method, [$($arg),*], [
            PhysicalRelOperator::Exchange($rel_op) => $rel_op.$method($($arg),*),
        ])
    };
    (@enum RelOperator, $self:ident, $rel_op:ident, $method:ident, $($arg:expr),*) => {
        $crate::impl_match_rel_op!(@dispatch RelOperator, $self, $rel_op, $method, [$($arg),*], [])
    };
    (@dispatch $operator:ident, $self:ident, $rel_op:ident, $method:ident, [$($arg:expr),*], [$($extra_arms:tt)*]) => {
        match $self {
            $operator::Scan($rel_op) => $rel_op.$method($($arg),*),
            $operator::Join($rel_op) => $rel_op.$method($($arg),*),
            $operator::EvalScalar($rel_op) => $rel_op.$method($($arg),*),
            $operator::Filter($rel_op) => $rel_op.$method($($arg),*),
            $operator::Aggregate($rel_op) => $rel_op.$method($($arg),*),
            $operator::Sort($rel_op) => $rel_op.$method($($arg),*),
            $operator::Limit($rel_op) => $rel_op.$method($($arg),*),
            $operator::TopN($rel_op) => $rel_op.$method($($arg),*),
            $operator::UnionAll($rel_op) => $rel_op.$method($($arg),*),
            $operator::DummyTableScan($rel_op) => $rel_op.$method($($arg),*),
            $operator::Window($rel_op) => $rel_op.$method($($arg),*),
            $operator::WindowGroup($rel_op) => $rel_op.$method($($arg),*),
            $operator::ProjectSet($rel_op) => $rel_op.$method($($arg),*),
            $operator::ConstantTableScan($rel_op) => $rel_op.$method($($arg),*),
            $operator::ExpressionScan($rel_op) => $rel_op.$method($($arg),*),
            $operator::CacheScan($rel_op) => $rel_op.$method($($arg),*),
            $operator::Udf($rel_op) => $rel_op.$method($($arg),*),
            $operator::RecursiveCteScan($rel_op) => $rel_op.$method($($arg),*),
            $operator::AsyncFunction($rel_op) => $rel_op.$method($($arg),*),
            $operator::Mutation($rel_op) => $rel_op.$method($($arg),*),
            $operator::CompactBlock($rel_op) => $rel_op.$method($($arg),*),
            $operator::MutationSource($rel_op) => $rel_op.$method($($arg),*),
            $operator::MaterializedCTE($rel_op) => $rel_op.$method($($arg),*),
            $operator::MaterializedCTERef($rel_op) => $rel_op.$method($($arg),*),
            $operator::Sequence($rel_op) => $rel_op.$method($($arg),*),
            $($extra_arms)*
        }
    }
}

#[macro_export]
macro_rules! match_rel_op {
    ($self:ident, $method:ident) => { impl_match_rel_op!($self, rel_op, $method,) };
    ($self:ident, $method:ident($($arg:expr),*)) => { impl_match_rel_op!($self, rel_op, $method, $($arg),*) };
}
