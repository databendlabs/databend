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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::StateSerdeItem;
use crate::Symbol;
use crate::aggregate::AggrState;
use crate::aggregate::AggrStateLoc;
use crate::aggregate::AggrStateType;
use crate::aggregate::StateAddr;
use crate::aggregate::StateSerdeType;
use crate::types::DataType;

pub type AggregateFunctionRef = Arc<dyn FunctionInstance>;
pub type AggregateFunctionBuildFn =
    fn(AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub params: Vec<Scalar>,
    pub args_type: Vec<DataType>,
    pub distinct: bool,
    pub order_by: Vec<AggregateBoundOrderByItem>,
    pub return_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateBoundOrderByItem {
    pub symbol: Symbol,
    pub source: AggregateBoundOrderBySource,
    pub data_type: DataType,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateBoundOrderBySource {
    Argument { index: usize },
    Derived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRuntimeOrderByItem {
    pub input: AggregateRuntimeOrderByInput,
    pub data_type: DataType,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateRuntimeOrderByInput {
    Argument { offset: usize },
    SortKey { offset: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateArgumentPattern {
    pub kind: AggregateArgumentKind,
    pub nullability: AggregateArgumentNullability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArgumentKind {
    Exact(DataType),
    AnyNumber,
    AnyDecimal,
    AnyNumeric,
    Any,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AggregateArgumentNullability {
    #[default]
    Any,
    NonNullable,
    Nullable,
}

impl AggregateArgumentPattern {
    pub fn exact(data_type: DataType) -> Self {
        Self {
            kind: AggregateArgumentKind::Exact(data_type),
            nullability: AggregateArgumentNullability::Any,
        }
    }

    pub fn any_number() -> Self {
        Self {
            kind: AggregateArgumentKind::AnyNumber,
            nullability: AggregateArgumentNullability::Any,
        }
    }

    pub fn any_decimal() -> Self {
        Self {
            kind: AggregateArgumentKind::AnyDecimal,
            nullability: AggregateArgumentNullability::Any,
        }
    }

    pub fn any_numeric() -> Self {
        Self {
            kind: AggregateArgumentKind::AnyNumeric,
            nullability: AggregateArgumentNullability::Any,
        }
    }

    pub fn any() -> Self {
        Self {
            kind: AggregateArgumentKind::Any,
            nullability: AggregateArgumentNullability::Any,
        }
    }

    pub fn non_nullable(mut self) -> Self {
        self.nullability = AggregateArgumentNullability::NonNullable;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.nullability = AggregateArgumentNullability::Nullable;
        self
    }

    pub fn matches_type(&self, data_type: &DataType) -> bool {
        let data_type = match self.nullability {
            AggregateArgumentNullability::Any if data_type.is_null() => return true,
            AggregateArgumentNullability::Any => data_type.remove_nullable(),
            AggregateArgumentNullability::NonNullable => {
                if data_type.is_nullable_or_null() {
                    return false;
                }
                data_type.clone()
            }
            AggregateArgumentNullability::Nullable => {
                let DataType::Nullable(inner) = data_type else {
                    return false;
                };
                (**inner).clone()
            }
        };

        match &self.kind {
            AggregateArgumentKind::Exact(expected) => expected == &data_type,
            AggregateArgumentKind::AnyNumber => matches!(data_type, DataType::Number(_)),
            AggregateArgumentKind::AnyDecimal => matches!(data_type, DataType::Decimal(_)),
            AggregateArgumentKind::AnyNumeric => {
                matches!(data_type, DataType::Number(_) | DataType::Decimal(_))
            }
            AggregateArgumentKind::Any => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateArgumentsPattern {
    Fixed(Vec<AggregateArgumentPattern>),
    OneOf(Vec<AggregateArgumentsPattern>),
    If(Box<AggregateArgumentsPattern>),
    Variadic {
        prefix: Vec<AggregateArgumentPattern>,
        repeated: AggregateArgumentPattern,
        min_repeats: usize,
        max_repeats: Option<usize>,
    },
}

impl AggregateArgumentsPattern {
    pub fn fixed(args: impl Into<Vec<AggregateArgumentPattern>>) -> Self {
        Self::Fixed(args.into())
    }

    pub fn one_of(patterns: impl Into<Vec<AggregateArgumentsPattern>>) -> Self {
        Self::OneOf(patterns.into())
    }

    pub fn if_condition(arguments: AggregateArgumentsPattern) -> Self {
        Self::If(Box::new(arguments))
    }

    pub fn variadic(
        prefix: impl Into<Vec<AggregateArgumentPattern>>,
        repeated: AggregateArgumentPattern,
        min_repeats: usize,
        max_repeats: Option<usize>,
    ) -> Self {
        Self::Variadic {
            prefix: prefix.into(),
            repeated,
            min_repeats,
            max_repeats,
        }
    }

    pub fn matches_types(&self, args_type: &[DataType]) -> bool {
        match self {
            Self::Fixed(args) => {
                args.len() == args_type.len()
                    && args
                        .iter()
                        .zip(args_type.iter())
                        .all(|(pattern, data_type)| pattern.matches_type(data_type))
            }
            Self::OneOf(patterns) => patterns
                .iter()
                .any(|pattern| pattern.matches_types(args_type)),
            Self::If(arguments) => {
                let Some((condition, nested_args)) = args_type.split_last() else {
                    return false;
                };
                AggregateArgumentPattern::exact(DataType::Boolean).matches_type(condition)
                    && arguments.matches_types(nested_args)
            }
            Self::Variadic {
                prefix,
                repeated,
                min_repeats,
                max_repeats,
            } => {
                let Some(repeats) = args_type.len().checked_sub(prefix.len()) else {
                    return false;
                };
                if repeats < *min_repeats || max_repeats.is_some_and(|max| repeats > max) {
                    return false;
                }
                prefix
                    .iter()
                    .zip(args_type.iter())
                    .all(|(pattern, data_type)| pattern.matches_type(data_type))
                    && args_type[prefix.len()..]
                        .iter()
                        .all(|data_type| repeated.matches_type(data_type))
            }
        }
    }
}

impl From<Vec<AggregateArgumentPattern>> for AggregateArgumentsPattern {
    fn from(args: Vec<AggregateArgumentPattern>) -> Self {
        Self::fixed(args)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortPolicy {
    #[default]
    Unsupported,
    Optional,
    Required,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DistinctPolicy {
    #[default]
    Unsupported,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FunctionFeatures {
    pub is_decomposable: bool,
    pub sort_policy: SortPolicy,
    pub distinct_policy: DistinctPolicy,
    pub category: &'static str,
    pub description: &'static str,
    pub definition: &'static str,
    pub example: &'static str,
}

#[derive(Debug, Clone, Default)]
pub struct AggregateStateDescription {
    fields: Vec<AggrStateType>,
    serde_items: Vec<StateSerdeItem>,
    need_manual_drop: bool,
}

impl AggregateStateDescription {
    pub fn new(
        fields: impl Into<Vec<AggrStateType>>,
        serde_items: impl Into<Vec<StateSerdeItem>>,
    ) -> Self {
        Self {
            fields: fields.into(),
            serde_items: serde_items.into(),
            need_manual_drop: false,
        }
    }

    pub fn with_manual_drop(mut self, need_manual_drop: bool) -> Self {
        self.need_manual_drop = need_manual_drop;
        self
    }

    pub fn with_null_flag(mut self) -> Self {
        self.fields.push(AggrStateType::Bool);
        self.serde_items
            .push(StateSerdeItem::DataType(DataType::Boolean));
        self
    }

    pub fn fields(&self) -> &[AggrStateType] {
        &self.fields
    }

    pub fn serde_items(&self) -> &[StateSerdeItem] {
        &self.serde_items
    }

    pub fn need_manual_drop(&self) -> bool {
        self.need_manual_drop
    }
}

pub(crate) fn state_at<T>(state: AggrState<'_>, index: usize) -> &mut T
where T: Send + 'static {
    state.addr.next(state.loc[index].offset()).get::<T>()
}

pub(crate) fn write_state_at<T>(state: AggrState<'_>, index: usize, value: T)
where T: Send + 'static {
    state
        .addr
        .next(state.loc[index].offset())
        .write_state(value)
}

pub struct AggregateStateSet<'a> {
    places: &'a [StateAddr],
    loc: &'a [AggrStateLoc],
}

impl<'a> AggregateStateSet<'a> {
    pub fn new(places: &'a [StateAddr], loc: &'a [AggrStateLoc]) -> Self {
        Self { places, loc }
    }

    pub fn len(&self) -> usize {
        self.places.len()
    }

    pub fn is_empty(&self) -> bool {
        self.places.is_empty()
    }

    pub fn get(&self, index: usize) -> AggrState<'_> {
        AggrState::new(self.places[index], self.loc)
    }

    pub fn without_first_loc(&self) -> AggregateStateSet<'a> {
        AggregateStateSet::new(self.places, &self.loc[1..])
    }

    pub fn without_last_loc(&self) -> AggregateStateSet<'a> {
        AggregateStateSet::new(self.places, &self.loc[..self.loc.len() - 1])
    }

    pub fn iter(&self) -> impl Iterator<Item = AggrState<'_>> {
        self.places
            .iter()
            .map(|place| AggrState::new(*place, self.loc))
    }
}

pub struct AccumulateInput<'a> {
    pub state: AggrState<'a>,
    pub columns: ProjectedBlock<'a>,
    pub validity: Option<&'a Bitmap>,
    pub order_by: &'a [AggregateRuntimeOrderByItem],
}

pub struct AccumulateKeysInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub columns: ProjectedBlock<'a>,
    pub order_by: &'a [AggregateRuntimeOrderByItem],
}

pub struct AccumulateRowInput<'a> {
    pub state: AggrState<'a>,
    pub columns: ProjectedBlock<'a>,
    pub row: usize,
}

pub struct AccumulateRowCountInput<'a> {
    pub state: AggrState<'a>,
    pub rows: usize,
}

pub struct AccumulateRowCountKeysInput<'a> {
    pub states: AggregateStateSet<'a>,
}

pub struct SerializeInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub builders: &'a mut [ColumnBuilder],
}

pub struct MergeSerializedInput<'a> {
    pub states: AggregateStateSet<'a>,
    pub state: &'a BlockEntry,
    pub filter: Option<&'a Bitmap>,
}

pub struct MergeStatesInput<'a> {
    pub state: AggrState<'a>,
    pub rhs: AggrState<'a>,
}

pub struct MergeResultInput<'a> {
    pub state: AggrState<'a>,
    pub builder: &'a mut ColumnBuilder,
}

pub trait AggrImpl: Send + Sync + 'static {
    fn init_state(&self, state: AggrState<'_>);

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()>;

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()>;

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()>;

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        if input.rows == 0 {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(
                "aggregate does not support rows-only input",
            ))
        }
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            self.accumulate_row_count(AccumulateRowCountInput { state, rows: 1 })?;
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()>;

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()>;

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()>;

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()>;

    /// Default for implementations whose result path does not consume state.
    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    /// # Safety
    /// The caller must ensure the state belongs to this aggregate
    /// implementation.
    unsafe fn drop_state(&self, state: AggrState<'_>);
}

pub trait FunctionInstance: fmt::Display + Send + Sync + 'static {
    fn signature(&self) -> &AggregateFunctionSignature;

    fn features(&self) -> &FunctionFeatures;

    fn state(&self) -> &AggregateStateDescription;

    fn init_state(&self, state: AggrState<'_>);

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()>;

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()>;

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()>;

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()>;

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()>;

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()>;

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()>;

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()>;

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()>;

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()>;

    fn state_data_type(&self) -> DataType {
        StateSerdeType::new(self.state().serde_items().to_vec()).data_type()
    }

    /// # Safety
    /// The caller must ensure the state belongs to this function.
    unsafe fn drop_state(&self, state: AggrState<'_>);
}

pub struct AggregateFunction<I> {
    signature: AggregateFunctionSignature,
    features: FunctionFeatures,
    state: AggregateStateDescription,
    implementation: I,
}

impl<I> AggregateFunction<I>
where I: AggrImpl
{
    pub fn new(
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Self {
        Self {
            signature,
            features,
            state,
            implementation,
        }
    }
}

impl<I> FunctionInstance for AggregateFunction<I>
where I: AggrImpl
{
    fn signature(&self) -> &AggregateFunctionSignature {
        &self.signature
    }

    fn features(&self) -> &FunctionFeatures {
        &self.features
    }

    fn state(&self) -> &AggregateStateDescription {
        &self.state
    }

    fn init_state(&self, state: AggrState<'_>) {
        self.implementation.init_state(state)
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        self.implementation.accumulate(input)
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        self.implementation.accumulate_keys(input)
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        self.implementation.accumulate_row(input)
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        self.implementation.accumulate_row_count(input)
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        self.implementation.accumulate_row_count_keys(input)
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        self.implementation.serialize(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        self.implementation.merge_serialized(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        self.implementation.merge_states(input)
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.implementation.merge_result(input)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        self.implementation.merge_result_read_only(input)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.implementation.drop_state(state) }
    }
}

impl<I> fmt::Display for AggregateFunction<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.signature.name)
    }
}

#[derive(Clone)]
pub struct AggregateFunctionRequest<'a> {
    pub name: &'a str,
    pub params: &'a [Scalar],
    pub args_type: &'a [DataType],
    pub distinct: bool,
    pub order_by: &'a [AggregateBoundOrderByItem],
}

pub trait AggregateFunctionBuilder: Send + Sync + 'static {
    fn arguments(&self) -> &AggregateArgumentsPattern;

    fn features(&self) -> &FunctionFeatures;

    fn build(&self, request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef>;
}

pub struct AggregateFunctionDescriptor {
    pub name: String,
    pub aliases: Vec<String>,
    arguments: AggregateArgumentsPattern,
    features: FunctionFeatures,
    builder: Arc<dyn AggregateFunctionBuilder>,
}

impl AggregateFunctionDescriptor {
    pub fn from_builder(
        name: impl Into<String>,
        builder: Arc<dyn AggregateFunctionBuilder>,
    ) -> Self {
        let arguments = builder.arguments().clone();
        let features = builder.features().clone();
        Self {
            name: name.into(),
            aliases: Vec::new(),
            arguments,
            features,
            builder,
        }
    }

    pub fn with_aliases(mut self, aliases: impl Into<Vec<String>>) -> Self {
        self.aliases = aliases.into();
        self
    }

    pub fn with_metadata(
        mut self,
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    ) -> Self {
        self.arguments = arguments;
        self.features = features;
        self
    }

    pub fn arguments(&self) -> &AggregateArgumentsPattern {
        &self.arguments
    }

    pub fn features(&self) -> &FunctionFeatures {
        &self.features
    }
}

#[derive(Default)]
pub struct AggregateFunctionRegistry {
    functions: HashMap<String, Vec<AggregateFunctionDescriptor>>,
    aliases: HashMap<String, String>,
}

impl AggregateFunctionRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn register(&mut self, descriptor: AggregateFunctionDescriptor) {
        let name = descriptor.name.to_lowercase();
        for alias in &descriptor.aliases {
            self.aliases.insert(alias.to_lowercase(), name.clone());
        }

        self.functions.entry(name).or_default().push(descriptor);
    }

    pub fn registered_names(&self) -> Vec<String> {
        self.functions
            .keys()
            .chain(self.aliases.keys())
            .unique()
            .cloned()
            .sorted()
            .collect()
    }

    pub fn aliases(&self) -> Vec<(&str, &str)> {
        self.aliases
            .iter()
            .map(|(alias, target)| (alias.as_str(), target.as_str()))
            .sorted_by_key(|(alias, _)| *alias)
            .collect()
    }

    pub fn descriptors(&self, name: &str) -> &[AggregateFunctionDescriptor] {
        let name = self.canonical_name(name);
        self.functions
            .get(name.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn contains(&self, name: &str) -> bool {
        !self.descriptors(name).is_empty()
    }

    pub fn contains_base(&self, name: &str) -> bool {
        let name = name.to_lowercase();
        self.functions.contains_key(&name)
    }

    pub fn is_decomposable(&self, name: &str) -> bool {
        self.descriptors(name)
            .iter()
            .any(|descriptor| descriptor.features().is_decomposable)
    }

    pub fn resolve(&self, request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
        let requested_name = request.name.to_lowercase();
        let name = self.canonical_name(request.name);
        let descriptors = self.functions.get(&name).ok_or_else(|| {
            ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction: {requested_name}"
            ))
        })?;

        let mut last_error = None;
        for descriptor in descriptors {
            if !descriptor.arguments().matches_types(request.args_type) {
                continue;
            }
            if !descriptor.features().sort_policy.accepts(request.order_by) {
                continue;
            }
            if !descriptor
                .features()
                .distinct_policy
                .accepts(request.distinct)
            {
                continue;
            }

            let request = AggregateFunctionRequest {
                name: descriptor.name.as_str(),
                params: request.params,
                args_type: request.args_type,
                distinct: request.distinct,
                order_by: request.order_by,
            };
            let builder = descriptor.builder.as_ref();
            match builder.build(request) {
                Ok(function) => return Ok(function),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction signature: {requested_name}({:?})",
                request.args_type
            ))
        }))
    }

    fn canonical_name(&self, name: &str) -> String {
        let name = name.to_lowercase();
        self.aliases.get(&name).cloned().unwrap_or(name)
    }
}

impl SortPolicy {
    fn accepts(&self, order_by: &[AggregateBoundOrderByItem]) -> bool {
        match self {
            Self::Unsupported => order_by.is_empty(),
            Self::Optional => true,
            Self::Required => !order_by.is_empty(),
        }
    }
}

impl DistinctPolicy {
    fn accepts(&self, distinct: bool) -> bool {
        match self {
            Self::Unsupported => !distinct,
            Self::Optional => true,
            Self::Required => distinct,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregateStatesLayout {
    pub layout: Layout,
    pub states_loc: Vec<Box<[AggrStateLoc]>>,
    pub serialize_type: Vec<StateSerdeType>,
}

pub fn get_states_layout(functions: &[AggregateFunctionRef]) -> Result<AggregateStatesLayout> {
    let mut states = Vec::new();
    let mut offsets = Vec::with_capacity(functions.len() + 1);
    let mut serialize_type = Vec::with_capacity(functions.len());
    offsets.push(0);

    for function in functions {
        states.extend_from_slice(function.state().fields());
        offsets.push(states.len());
        serialize_type.push(StateSerdeType::new(function.state().serde_items().to_vec()));
    }

    let (layout, locs) = sort_states(states);
    let states_loc = offsets
        .windows(2)
        .map(|window| locs[window[0]..window[1]].to_vec().into_boxed_slice())
        .collect();

    Ok(AggregateStatesLayout {
        layout,
        states_loc,
        serialize_type,
    })
}

impl From<AggregateStatesLayout> for crate::aggregate::StatesLayout {
    fn from(layout: AggregateStatesLayout) -> Self {
        crate::aggregate::StatesLayout {
            layout: layout.layout,
            states_loc: layout.states_loc,
            serialize_type: layout.serialize_type,
        }
    }
}

pub struct AggregateStateOwner {
    addr: StateAddr,
    layout: AggregateStatesLayout,
    functions: Vec<AggregateFunctionRef>,
    _arena: Bump,
}

impl AggregateStateOwner {
    pub fn new(functions: Vec<AggregateFunctionRef>) -> Result<Self> {
        let layout = get_states_layout(&functions)?;
        let _arena = Bump::new();
        let addr = _arena.alloc_layout(layout.layout).into();

        let owner = Self {
            addr,
            layout,
            functions,
            _arena,
        };

        for (index, function) in owner.functions.iter().enumerate() {
            function.init_state(owner.state(index));
        }

        Ok(owner)
    }

    pub fn state(&self, index: usize) -> AggrState<'_> {
        AggrState::new(self.addr, &self.layout.states_loc[index])
    }

    pub fn state_set(&self, index: usize) -> AggregateStateSet<'_> {
        AggregateStateSet::new(
            std::slice::from_ref(&self.addr),
            &self.layout.states_loc[index],
        )
    }
}

impl Drop for AggregateStateOwner {
    fn drop(&mut self) {
        for (index, function) in self.functions.iter().enumerate() {
            if function.state().need_manual_drop() {
                unsafe {
                    function.drop_state(AggrState::new(self.addr, &self.layout.states_loc[index]));
                }
            }
        }
    }
}

fn sort_states(states: Vec<AggrStateType>) -> (Layout, Vec<AggrStateLoc>) {
    if states.is_empty() {
        return (Layout::from_size_align(0, 1).unwrap(), Vec::new());
    }

    let mut states = states
        .iter()
        .enumerate()
        .map(|(idx, state)| {
            let layout = match state {
                AggrStateType::Bool => (1, 1),
                AggrStateType::Custom(layout) => (layout.align(), layout.pad_to_align().size()),
            };
            (idx, state, layout)
        })
        .collect::<Vec<_>>();

    states.sort_by_key(|(_, _, (align, _))| std::cmp::Reverse(*align));

    let mut locs = vec![AggrStateLoc::Bool(0, 0); states.len()];
    let mut acc = 0;
    let mut max_align = 0;
    for (idx, state, (align, size)) in states {
        max_align = max_align.max(align);
        let offset = acc;
        acc += size;
        locs[idx] = match state {
            AggrStateType::Bool => AggrStateLoc::Bool(idx, offset),
            AggrStateType::Custom(_) => AggrStateLoc::Custom(idx, offset),
        };
    }

    let layout = Layout::from_size_align(acc, max_align).unwrap();

    (layout, locs)
}
