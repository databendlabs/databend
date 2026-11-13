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

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::MutableBitmap;

use crate::topk_count::IS_TOPK_COUNT_CANDIDATE;
use crate::topk_count::TopKCountKey;
use crate::topk_count::decode_candidates;
use crate::topk_count::with_topk_count_key_type;

/// Registers `is_topk_count_candidate(candidates, key)`.
///
/// `candidates` is the output of `topk_count_candidates`. The function
/// returns true for the keys in the candidate set and for NULL keys, and for
/// every key if there is no candidate set, see `crate::topk_count`.
pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory(
        IS_TOPK_COUNT_CANDIDATE,
        FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
            let [candidates_type, key_type] = args_type else {
                return None;
            };
            if !matches!(
                candidates_type.remove_nullable(),
                DataType::Binary | DataType::Null
            ) {
                return None;
            }
            let eval: Box<dyn ScalarFunction> = with_topk_count_key_type!(
                key_type.remove_nullable(),
                |K| Box::new(CandidateFilter::<K>(PhantomData)),
                return None
            );
            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: IS_TOPK_COUNT_CANDIDATE.to_string(),
                    args_type: args_type.to_vec(),
                    return_type: DataType::Boolean,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(FunctionDomain::<AnyType>::Full),
                    derive_stat: None,
                    eval,
                },
            }))
        })),
    );
}

struct CandidateFilter<K>(PhantomData<fn(K)>);

/// The decoded candidate set; `None` keeps every key.
type CandidateSet<K> = Option<KeySet<K>>;

type KeySet<K> = HashSet<<K as TopKCountKey>::Key, ahash::RandomState>;

impl<K: TopKCountKey> CandidateFilter<K> {
    fn decode(candidates: ScalarRef<'_>) -> Result<CandidateSet<K>, String> {
        match candidates {
            ScalarRef::Null => Ok(None),
            ScalarRef::Binary(data) => decode_candidates::<K::Key>(data)
                .map(|keys| keys.map(|keys| keys.into_iter().collect()))
                .map_err(|err| format!("{IS_TOPK_COUNT_CANDIDATE} got invalid candidates: {err}")),
            other => Err(format!(
                "{IS_TOPK_COUNT_CANDIDATE} expects binary candidates, got {other:?}"
            )),
        }
    }

    fn is_candidate(set: &CandidateSet<K>, key: ScalarRef<'_>) -> bool {
        let Some(set) = set else {
            return true;
        };
        if key == ScalarRef::Null {
            return true;
        }
        let key = K::try_downcast_scalar(&key).unwrap();
        set.contains(K::lookup(&key))
    }

    /// Filters a whole key column by one candidate set.
    fn filter_column(set: &KeySet<K>, keys: &Column) -> Column {
        let (keys, validity) = match keys {
            Column::Nullable(nullable) => (nullable.column(), Some(nullable.validity())),
            keys => (keys, None),
        };
        let keys = K::try_downcast_column(keys).unwrap();
        let keys = K::iter_column(&keys);
        let bitmap = match validity {
            None => MutableBitmap::from_iter(keys.map(|key| set.contains(K::lookup(&key)))),
            Some(validity) => MutableBitmap::from_iter(
                keys.zip(validity.iter())
                    .map(|(key, valid)| !valid || set.contains(K::lookup(&key))),
            ),
        };
        Column::Boolean(bitmap.into())
    }
}

impl<K: TopKCountKey> ScalarFunction for CandidateFilter<K> {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        match (&args[0], &args[1]) {
            // The planner passes the candidates as a constant column.
            (Value::Scalar(candidates), keys) => match Self::decode(candidates.as_ref()) {
                Ok(None) => Value::Scalar(Scalar::Boolean(true)),
                Ok(Some(set)) => match keys {
                    Value::Scalar(key) => Value::Scalar(Scalar::Boolean(Self::is_candidate(
                        &Some(set),
                        key.as_ref(),
                    ))),
                    Value::Column(keys) => Value::Column(Self::filter_column(&set, keys)),
                },
                Err(err) => {
                    ctx.set_error(0, err);
                    Value::Scalar(Scalar::Boolean(false))
                }
            },
            (Value::Column(candidates), keys) => {
                let mut decoded: Option<(ScalarRef<'_>, CandidateSet<K>)> = None;
                let mut builder = MutableBitmap::with_capacity(ctx.num_rows);
                for row in 0..ctx.num_rows {
                    let candidates = candidates.index(row).unwrap();
                    if decoded.as_ref().is_none_or(|(last, _)| *last != candidates) {
                        match Self::decode(candidates.clone()) {
                            Ok(set) => decoded = Some((candidates, set)),
                            Err(err) => {
                                ctx.set_error(row, err);
                                return Value::Scalar(Scalar::Boolean(false));
                            }
                        }
                    }
                    let (_, set) = decoded.as_ref().unwrap();
                    builder.push(Self::is_candidate(set, keys.index(row).unwrap()));
                }
                Value::Column(Column::Boolean(builder.into()))
            }
        }
    }
}
