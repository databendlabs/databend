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

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

use crate::table::SystemTablePart;

pub trait SystemLogElement: Send + Sync + Clone {
    const TABLE_NAME: &'static str;

    fn schema() -> TableSchemaRef;

    fn fill_to_data_block(&self, columns: &mut Vec<ColumnBuilder>) -> Result<()>;
}

struct Data<Event: SystemLogElement> {
    index: usize,
    event_queue: Vec<Option<Event>>,
}

impl<Event: SystemLogElement> Data<Event> {
    pub fn new(size: usize) -> Data<Event> {
        Data::<Event> {
            index: 0,
            event_queue: vec![None; size],
        }
    }
}

pub struct SystemLogQueue<Event: SystemLogElement> {
    max_rows: usize,
    data: Arc<RwLock<Data<Event>>>,
}

static INSTANCES_MAP: OnceCell<RwLock<HashMap<TypeId, Box<dyn Any + 'static + Send + Sync>>>> =
    OnceCell::new();

impl<Event: SystemLogElement + 'static> SystemLogQueue<Event> {
    pub fn init(max_rows: usize) {
        let instance: Box<dyn Any + 'static + Send + Sync> = Box::new(Self::create(max_rows));

        let instances_map = INSTANCES_MAP.get_or_init(move || RwLock::new(HashMap::new()));
        let mut write_guard = instances_map.write();
        write_guard.insert(TypeId::of::<Self>(), instance);
    }

    pub fn instance() -> Result<Arc<SystemLogQueue<Event>>> {
        unsafe {
            match INSTANCES_MAP
                .get_unchecked()
                .read()
                .get(&TypeId::of::<Self>())
            {
                None => Err(ErrorCode::Internal("")),
                Some(instance) => instance
                    .downcast_ref::<Arc<Self>>()
                    .cloned()
                    .ok_or(ErrorCode::Internal("")),
            }
        }
    }

    pub fn create(max_rows: usize) -> Arc<SystemLogQueue<Event>> {
        Arc::new(SystemLogQueue::<Event> {
            max_rows,
            data: Arc::new(RwLock::new(Data::new(max_rows))),
        })
    }

    pub fn append_data(&self, event: Event) -> Result<()> {
        let mut write_guard = self.data.write();
        let cur_index = write_guard.index;
        write_guard.event_queue[cur_index] = Some(event);
        write_guard.index += 1;

        if write_guard.index == self.max_rows {
            write_guard.index = 0;
        }

        Ok(())
    }
}

pub struct SystemLogTable<Event: SystemLogElement> {
    table_info: TableInfo,
    _phantom_data: PhantomData<Event>,
}

impl<Event: SystemLogElement + 'static> SystemLogTable<Event> {
    pub fn create(table_id: u64, max_rows: usize) -> Self {
        let table_info = TableInfo {
            desc: format!("'system'.'{}'", Event::TABLE_NAME),
            name: Event::TABLE_NAME.to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: Event::schema(),
                engine: "SystemLogTable".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SystemLogQueue::<Event>::init(max_rows);

        SystemLogTable::<Event> {
            table_info,
            _phantom_data: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<Event: SystemLogElement + 'static> Table for SystemLogTable<Event> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::default(),
            // Make the table in distributed.
            Partitions::create_nolazy(PartitionsShuffleKind::Broadcast, vec![Arc::new(Box::new(
                SystemTablePart,
            ))]),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let schema = Event::schema();
        let mut mutable_columns: Vec<ColumnBuilder> = Vec::with_capacity(schema.num_fields());
        let mut data_types: Vec<DataType> = Vec::with_capacity(schema.num_fields());

        for column_field in schema.fields() {
            let data_type: DataType = column_field.data_type().into();
            let mutable_column = ColumnBuilder::with_capacity(&data_type, 0);
            mutable_columns.push(mutable_column);
            data_types.push(data_type);
        }

        let log_queue = SystemLogQueue::<Event>::instance()?;
        for event in log_queue.data.read().event_queue.iter().flatten() {
            event.fill_to_data_block(&mut mutable_columns)?;
        }

        let mut columns = Vec::with_capacity(mutable_columns.len());
        for mutable_column in mutable_columns.into_iter() {
            columns.push(mutable_column.build());
        }

        // Add source pipe.
        pipeline.add_source(
            move |output| {
                SystemLogSource::<Event>::create(
                    ctx.clone(),
                    output,
                    DataBlock::new_from_columns(columns.clone()),
                )
            },
            1,
        )
    }

    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _: bool) -> Result<()> {
        let log_queue = SystemLogQueue::<Event>::instance()?;
        let mut write_guard = log_queue.data.write();

        write_guard.index = 0;
        for index in 0..write_guard.event_queue.len() {
            write_guard.event_queue[index] = None;
        }

        Ok(())
    }
}

struct SystemLogSource<Event: SystemLogElement> {
    data: Option<DataBlock>,
    _phantom: PhantomData<Event>,
}

impl<Event: SystemLogElement + 'static> SystemLogSource<Event> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        data: DataBlock,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, Self {
            data: Some(data),
            _phantom: Default::default(),
        })
    }
}

impl<Event: SystemLogElement + 'static> SyncSource for SystemLogSource<Event> {
    const NAME: &'static str = Event::TABLE_NAME;

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.data.take())
    }
}
