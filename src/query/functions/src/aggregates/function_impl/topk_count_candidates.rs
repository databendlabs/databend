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

use std::alloc::Layout;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;

use super::AggregateRegistration;
use super::adaptors::*;
use crate::topk_count::HeavyHitterSummary;
use crate::topk_count::TOPK_COUNT_CANDIDATES;
use crate::topk_count::TopKCountKey;
use crate::topk_count::encode_candidates;
use crate::topk_count::with_topk_count_key_type;

struct TopKCountCandidatesBuilder;

impl TopKCountCandidatesBuilder {
    fn register(registry: &mut AggregateRegistry) {
        NameRoute::new(
            &[TOPK_COUNT_CANDIDATES],
            ArgumentsPattern::fixed(vec![ArgumentPattern::any()]),
            Self::METADATA,
            NullInput::Filter,
        )
        // Planned by the optimizer only, see `crate::topk_count`.
        .then(PlainRoute::unary(Self::create).hidden())
        .register(registry);
    }

    const METADATA: AggregateMetadata = AggregateMetadata {
        null_argument_result: NullArgumentResult::Null,
        eager_aggregation: EagerAggregation::Unsupported,
        sort_policy: SortPolicy::Unsupported,
        documentation: AggregateDocumentation {
            category: "Aggregate",
            description: "returns the keys that may be among the most frequent keys",
            definition: "topk_count_candidates(limit, capacity)(expr)",
            example: "select topk_count_candidates(10, 4096)(number % 7) from numbers(100)",
        },
    };

    fn create(build: UnaryBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let info = Self::parse_params(build.name(), build.params())?;
        let data_type = build.arg_type().clone();
        with_topk_count_key_type!(
            &data_type,
            |K| build.create_unary::<TopKCountCandidatesState<K>, K, BinaryType>(
                DataType::Binary,
                TopKCountCandidatesState::<K>::state_description(),
                info,
            ),
            Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{}'",
                build.name(),
                data_type
            )))
        )
    }

    fn parse_params(name: &str, params: &[Scalar]) -> Result<TopKCountCandidatesInfo> {
        let parse = |param: &Scalar| match param {
            Scalar::Number(number) => number
                .integer_to_i128()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0),
            _ => None,
        };
        match params {
            [limit, capacity] => match (parse(limit), parse(capacity)) {
                (Some(limit), Some(capacity)) if capacity >= limit => {
                    Ok(TopKCountCandidatesInfo { limit, capacity })
                }
                _ => Err(ErrorCode::BadArguments(format!(
                    "{name} expects positive integer parameters limit <= capacity"
                ))),
            },
            _ => Err(ErrorCode::BadArguments(format!(
                "{name} expects two parameters: limit and capacity"
            ))),
        }
    }
}

inventory::submit! {
    AggregateRegistration {
        register: TopKCountCandidatesBuilder::register,
    }
}

struct TopKCountCandidatesInfo {
    limit: usize,
    capacity: usize,
}

struct TopKCountCandidatesState<K: TopKCountKey> {
    summary: HeavyHitterSummary<K::Key>,
}

impl<K: TopKCountKey> TopKCountCandidatesState<K> {
    fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }
}

impl<K: TopKCountKey> UnaryState<K, BinaryType> for TopKCountCandidatesState<K> {
    type FunctionInfo = TopKCountCandidatesInfo;

    fn init(info: &Self::FunctionInfo) -> Self {
        Self {
            summary: HeavyHitterSummary::new(info.capacity),
        }
    }

    fn add(&mut self, value: K::ScalarRef<'_>, _info: &Self::FunctionInfo) -> Result<()> {
        self.summary
            .add(K::lookup(&value), || K::to_key(value.clone()));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.summary.merge(&rhs.summary);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.summary.merge_owned(&mut rhs.summary);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <BinaryType as ValueType>::ColumnBuilderMut<'_>,
        info: &Self::FunctionInfo,
    ) -> Result<()> {
        let candidates = self.summary.candidates(info.limit);
        encode_candidates(&candidates, &mut builder.data)?;
        builder.commit_row();
        Ok(())
    }

    fn serialize(&self, builder: &mut ColumnBuilder, _info: &Self::FunctionInfo) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(&self.summary, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>, _info: &Self::FunctionInfo) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            return Err(ErrorCode::Internal(format!(
                "{TOPK_COUNT_CANDIDATES} expects a binary state, got {value:?}"
            )));
        };
        let mut rhs = HeavyHitterSummary::<K::Key>::deserialize_reader(&mut data)?;
        self.summary.merge_owned(&mut rhs);
        Ok(())
    }
}
