//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::catalog::CATALOG_DEFAULT;
use common_datablocks::assert_blocks_sorted_eq_with_name;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SourcePipeBuilder;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::Expression;
use common_planners::Extras;
use common_storage::StorageFsConfig;
use common_storage::StorageParams;
use common_streams::SendableDataBlockStream;
use databend_query::interpreters::append2table;
use databend_query::interpreters::commit2table;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactoryV2;
use databend_query::pipelines::processors::BlocksSource;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;
use databend_query::sql::OPT_KEY_DATABASE_ID;
use databend_query::storages::fuse::table_functions::ClusteringInformationTable;
use databend_query::storages::fuse::table_functions::FuseSnapshotTable;
use databend_query::storages::fuse::FUSE_TBL_BLOCK_INDEX_PREFIX;
use databend_query::storages::fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_query::storages::fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_query::storages::fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_query::storages::Table;
use databend_query::storages::TableStreamReadWrap;
use databend_query::storages::ToReadDataSourcePlan;
use databend_query::table_functions::TableArgs;
use futures::TryStreamExt;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio_stream::StreamExt;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::tests::TestGuard;

pub struct TestFixture {
    _tmp_dir: TempDir,
    ctx: Arc<QueryContext>,
    _guard: TestGuard,
    prefix: String,
}

impl TestFixture {
    pub async fn new() -> TestFixture {
        let tmp_dir = TempDir::new().unwrap();
        let mut conf = crate::tests::ConfigBuilder::create().config();

        // make sure we are suing `fs` storage
        conf.storage.params = StorageParams::Fs(StorageFsConfig {
            // use `TempDir` as root path (auto clean)
            root: tmp_dir.path().to_str().unwrap().to_string(),
        });

        let (_guard, ctx) = crate::tests::create_query_context_with_config(conf, None)
            .await
            .unwrap();

        let tenant = ctx.get_tenant();
        let random_prefix: String = Uuid::new_v4().simple().to_string();
        // prepare a randomly named default database
        let db_name = gen_db_name(&random_prefix);
        let plan = CreateDatabasePlan {
            catalog: "default".to_owned(),
            tenant,
            if_not_exists: false,
            database: db_name,
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..Default::default()
            },
        };
        ctx.get_catalog("default")
            .unwrap()
            .create_database(plan.into())
            .await
            .unwrap();

        Self {
            _tmp_dir: tmp_dir,
            ctx,
            _guard,
            prefix: random_prefix,
        }
    }

    pub fn ctx(&self) -> Arc<QueryContext> {
        self.ctx.clone()
    }

    pub fn default_tenant(&self) -> String {
        self.ctx().get_tenant()
    }

    pub fn default_db_name(&self) -> String {
        gen_db_name(&self.prefix)
    }

    pub fn default_catalog_name(&self) -> String {
        "default".to_owned()
    }

    pub fn default_table_name(&self) -> String {
        format!("tbl_{}", self.prefix)
    }

    pub fn default_schema() -> DataSchemaRef {
        let tuple_inner_names = vec!["a".to_string(), "b".to_string()];
        let tuple_inner_data_types = vec![i32::to_data_type(), i32::to_data_type()];
        let tuple_data_type = StructType::new_impl(Some(tuple_inner_names), tuple_inner_data_types);
        DataSchemaRefExt::create(vec![
            DataField::new("id", i32::to_data_type()),
            DataField::new("t", tuple_data_type),
        ])
    }

    pub fn default_crate_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            table_meta: TableMeta {
                schema: TestFixture::default_schema(),
                engine: "FUSE".to_string(),
                options: [
                    // database id is required for FUSE
                    (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
                ]
                .into(),
                default_cluster_key: Some("(id)".to_string()),
                cluster_keys: vec!["(id)".to_string()],
                default_cluster_key_id: Some(0),
                ..Default::default()
            },
            as_select: None,
            cluster_keys: vec!["id".to_string()],
        }
    }

    // create a normal table without cluster key.
    pub fn create_normal_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            table_meta: TableMeta {
                schema: TestFixture::default_schema(),
                engine: "FUSE".to_string(),
                options: [
                    // database id is required for FUSE
                    (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
                ]
                .into(),
                ..Default::default()
            },
            as_select: None,
            cluster_keys: vec![],
        }
    }

    pub async fn create_default_table(&self) -> Result<()> {
        let create_table_plan = self.default_crate_table_plan();
        let interpreter = CreateTableInterpreter::try_create(self.ctx.clone(), create_table_plan)?;
        interpreter.execute().await?;
        Ok(())
    }

    pub fn gen_sample_blocks(num: usize, start: i32) -> Vec<Result<DataBlock>> {
        Self::gen_sample_blocks_ex(num, 3, start)
    }

    pub fn gen_sample_blocks_ex(
        num_of_block: usize,
        rows_perf_block: usize,
        start: i32,
    ) -> Vec<Result<DataBlock>> {
        (0..num_of_block)
            .into_iter()
            .map(|idx| {
                let tuple_inner_names = vec!["a".to_string(), "b".to_string()];
                let tuple_inner_data_types = vec![i32::to_data_type(), i32::to_data_type()];
                let tuple_data_type =
                    StructType::new_impl(Some(tuple_inner_names), tuple_inner_data_types);
                let schema = DataSchemaRefExt::create(vec![
                    DataField::new("id", i32::to_data_type()),
                    DataField::new("t", tuple_data_type.clone()),
                ]);
                let column0 = Series::from_data(
                    std::iter::repeat_with(|| idx as i32 + start)
                        .take(rows_perf_block)
                        .collect::<Vec<i32>>(),
                );
                let column1 = Series::from_data(
                    std::iter::repeat_with(|| (idx as i32 + start) * 2)
                        .take(rows_perf_block)
                        .collect::<Vec<i32>>(),
                );
                let column2 = Series::from_data(
                    std::iter::repeat_with(|| (idx as i32 + start) * 3)
                        .take(rows_perf_block)
                        .collect::<Vec<i32>>(),
                );
                let tuple_inner_columns = vec![column1, column2];
                let tuple_column =
                    StructColumn::from_data(tuple_inner_columns, tuple_data_type).arc();

                Ok(DataBlock::create(schema, vec![column0, tuple_column]))
            })
            .collect()
    }

    pub fn gen_sample_blocks_stream(num: usize, start: i32) -> SendableDataBlockStream {
        let blocks = Self::gen_sample_blocks(num, start);
        Box::pin(futures::stream::iter(blocks))
    }

    pub fn gen_sample_blocks_stream_ex(
        num_of_block: usize,
        rows_perf_block: usize,
        val_start_from: i32,
    ) -> SendableDataBlockStream {
        let blocks = Self::gen_sample_blocks_ex(num_of_block, rows_perf_block, val_start_from);
        Box::pin(futures::stream::iter(blocks))
    }

    pub async fn latest_default_table(&self) -> Result<Arc<dyn Table>> {
        self.ctx
            .get_catalog(CATALOG_DEFAULT)?
            .get_table(
                self.default_tenant().as_str(),
                self.default_db_name().as_str(),
                self.default_table_name().as_str(),
            )
            .await
    }

    /// append_commit_blocks with single thread
    pub async fn append_commit_blocks(
        &self,
        table: Arc<dyn Table>,
        blocks: Vec<DataBlock>,
        overwrite: bool,
        commit: bool,
    ) -> Result<()> {
        let source_schema = blocks
            .get(0)
            .map(|b| b.schema().clone())
            .unwrap_or_else(|| table.schema());
        let mut pipeline = Pipeline::create();
        let mut builder = SourcePipeBuilder::create();

        let blocks = Arc::new(Mutex::new(VecDeque::from_iter(blocks)));
        let output = OutputPort::create();
        builder.add_source(
            output.clone(),
            BlocksSource::create(self.ctx.clone(), output.clone(), blocks.clone())?,
        );
        pipeline.add_pipe(builder.finalize());

        append2table(
            self.ctx.clone(),
            table.clone(),
            source_schema,
            PipelineBuildResult {
                main_pipeline: pipeline,
                sources_pipelines: vec![],
            },
        )?;

        if commit {
            commit2table(self.ctx.clone(), table.clone(), overwrite).await?;
        }
        Ok(())
    }
}

fn gen_db_name(prefix: &str) -> String {
    format!("db_{}", prefix)
}

pub async fn test_drive(
    ctx: Arc<QueryContext>,
    test_db: Option<&str>,
    test_tbl: Option<&str>,
) -> Result<()> {
    let arg_db = match test_db {
        Some(v) => DataValue::String(v.as_bytes().to_vec()),
        None => DataValue::Null,
    };

    let arg_tbl = match test_tbl {
        Some(v) => DataValue::String(v.as_bytes().to_vec()),
        None => DataValue::Null,
    };

    let tbl_args = Some(vec![
        Expression::create_literal(arg_db),
        Expression::create_literal(arg_tbl),
    ]);

    test_drive_with_args(ctx, tbl_args).await
}

pub async fn test_drive_with_args(ctx: Arc<QueryContext>, tbl_args: TableArgs) -> Result<()> {
    let mut stream = test_drive_with_args_and_ctx(tbl_args, ctx).await?;

    while let Some(res) = stream.next().await {
        if let Err(cause) = res {
            return Err(cause);
        }
    }

    Ok(())
}

pub async fn test_drive_with_args_and_ctx(
    tbl_args: TableArgs,
    ctx: Arc<QueryContext>,
) -> Result<SendableDataBlockStream> {
    let func = FuseSnapshotTable::create("system", "fuse_snapshot", 1, tbl_args)?;
    let source_plan = func
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    func.as_table().read(ctx, &source_plan).await
}

pub async fn test_drive_clustering_information(
    tbl_args: TableArgs,
    ctx: Arc<QueryContext>,
) -> Result<SendableDataBlockStream> {
    let func = ClusteringInformationTable::create("system", "clustering_information", 1, tbl_args)?;
    let source_plan = func
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    func.as_table().read(ctx, &source_plan).await
}

pub fn expects_err<T>(case_name: &str, err_code: u16, res: Result<T>) {
    if let Err(err) = res {
        assert_eq!(
            err.code(),
            err_code,
            "case name {}, unexpected error: {}",
            case_name,
            err
        );
    } else {
        panic!(
            "case name {}, expecting err code {}, but got ok",
            case_name, err_code,
        );
    }
}

pub async fn expects_ok(
    case_name: impl AsRef<str>,
    res: Result<SendableDataBlockStream>,
    expected: Vec<&str>,
) -> Result<()> {
    match res {
        Ok(stream) => {
            let blocks: Vec<DataBlock> = stream.try_collect().await?;
            assert_blocks_sorted_eq_with_name(case_name.as_ref(), expected, &blocks)
        }
        Err(err) => {
            panic!(
                "case name {}, expecting  Ok, but got err {}",
                case_name.as_ref(),
                err,
            )
        }
    };
    Ok(())
}

pub async fn execute_query(ctx: Arc<QueryContext>, query: &str) -> Result<SendableDataBlockStream> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
    executor.execute().await
}

pub async fn execute_command(ctx: Arc<QueryContext>, query: &str) -> Result<()> {
    let res = execute_query(ctx, query).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

pub async fn append_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    append_sample_data_overwrite(num_blocks, false, fixture).await
}

pub async fn append_sample_data_overwrite(
    num_blocks: usize,
    overwrite: bool,
    fixture: &TestFixture,
) -> Result<()> {
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, overwrite, true)
        .await
}

pub async fn check_data_dir(
    fixture: &TestFixture,
    case_name: &str,
    snapshot_count: u32,
    segment_count: u32,
    block_count: u32,
    index_count: u32,
) {
    let data_path = match fixture.ctx().get_config().storage.params {
        StorageParams::Fs(v) => v.root,
        _ => panic!("storage type is not fs"),
    };
    let root = data_path.as_str();
    let mut ss_count = 0;
    let mut sg_count = 0;
    let mut b_count = 0;
    let mut i_count = 0;
    let prefix_snapshot = FUSE_TBL_SNAPSHOT_PREFIX;
    let prefix_segment = FUSE_TBL_SEGMENT_PREFIX;
    let prefix_block = FUSE_TBL_BLOCK_PREFIX;
    let prefix_index = FUSE_TBL_BLOCK_INDEX_PREFIX;
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            // here, by checking if "contains" the prefix is enough
            if entry.path().to_str().unwrap().contains(prefix_snapshot) {
                ss_count += 1;
            } else if entry.path().to_str().unwrap().contains(prefix_segment) {
                sg_count += 1;
            } else if entry.path().to_str().unwrap().contains(prefix_block) {
                b_count += 1;
            } else if entry.path().to_str().unwrap().contains(prefix_index) {
                i_count += 1;
            }
        }
    }

    assert_eq!(
        ss_count, snapshot_count,
        "case [{}], check snapshot count",
        case_name
    );
    assert_eq!(
        sg_count, segment_count,
        "case [{}], check segment count",
        case_name
    );

    assert_eq!(
        b_count, block_count,
        "case [{}], check block count",
        case_name
    );

    assert_eq!(
        i_count, index_count,
        "case [{}], check index count",
        case_name
    );
}

pub async fn history_should_have_only_one_item(
    fixture: &TestFixture,
    case_name: &str,
) -> Result<()> {
    // check history
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let expected = vec![
        "+-------+",
        "| count |",
        "+-------+",
        "| 1     |",
        "+-------+",
    ];
    let qry = format!(
        "select count(*) as count from fuse_snapshot('{}', '{}')",
        db, tbl
    );

    expects_ok(
        format!("{}: count_of_history_item_should_be_1", case_name),
        execute_query(fixture.ctx(), qry.as_str()).await,
        expected,
    )
    .await
}
