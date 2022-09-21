use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::MutableColumn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::Extras;
use common_legacy_planners::Partitions;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::Statistics;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_pipeline_sources::processors::sources::SyncSource;
use common_pipeline_sources::processors::sources::SyncSourcer;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;

pub trait SystemLogElement: Send + Sync + Clone {
    const TABLE_NAME: &'static str;

    fn schema() -> DataSchemaRef;

    fn fill_to_data_block(&self, columns: &mut Vec<Box<dyn MutableColumn>>) -> Result<()>;
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
                None => Err(ErrorCode::LogicalError("")),
                Some(instance) => match instance.downcast_ref::<Arc<Self>>() {
                    None => Err(ErrorCode::LogicalError("")),
                    Some(instant) => Ok(instant.clone()),
                },
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
                engine: "SystemQueryTable".to_string(),
                ..Default::default()
            },
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

    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let mut source_builder = SourcePipeBuilder::create();

        let schema = Event::schema();
        let mut mutable_columns: Vec<Box<dyn MutableColumn>> =
            Vec::with_capacity(schema.num_fields());

        for column_field in schema.fields() {
            mutable_columns.push(column_field.data_type().create_mutable(0));
        }

        let log_queue = SystemLogQueue::<Event>::instance()?;
        for event in log_queue.data.read().event_queue.iter() {
            if let Some(event) = event {
                event.fill_to_data_block(&mut mutable_columns)?;
            }
        }

        let mut columns = Vec::with_capacity(mutable_columns.len());
        for mut mutable_column in mutable_columns.into_iter() {
            columns.push(mutable_column.to_column());
        }

        source_builder.add_source(
            output.clone(),
            SystemLogSource::<Event>::create(ctx, output, DataBlock::create(schema, columns))?,
        );

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
    }

    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _: &str, _: bool) -> Result<()> {
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
