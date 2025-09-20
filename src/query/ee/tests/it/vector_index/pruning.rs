// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_ast::ast::Engine;
use databend_common_base::base::tokio;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::VectorDataType;
use databend_common_expression::types::F32;
use databend_common_expression::Column;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::BloomIndexColumns;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::pruning::FusePruner;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_query::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_query::test_kits::*;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use opendal::Operator;

async fn apply_block_pruning(
    table_snapshot: Arc<TableSnapshot>,
    schema: TableSchemaRef,
    push_down: &Option<PushDownInfo>,
    ctx: Arc<QueryContext>,
    dal: Operator,
    bloom_index_cols: BloomIndexColumns,
) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
    let ctx: Arc<dyn TableContext> = ctx;
    let segment_locs = table_snapshot.segments.clone();
    let segment_locs = create_segment_location_vector(segment_locs, None);

    FusePruner::create(&ctx, dal, schema, push_down, bloom_index_cols, vec![], None)?
        .read_pruning(segment_locs)
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_block_pruner() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;

    let test_tbl_name = "test_vector_index";
    let test_schema = TableSchemaRefExt::create(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new(
            "embedding",
            TableDataType::Vector(VectorDataType::Float32(4)),
        ),
    ]);

    let row_per_block = 10;
    let num_blocks_opt = row_per_block.to_string();

    let index_name = "idx1".to_string();
    let index_version = "test1".to_string();

    let mut index_options = BTreeMap::new();
    index_options.insert("m".to_string(), "10".to_string());
    index_options.insert("ef_construct".to_string(), "40".to_string());
    index_options.insert("distance".to_string(), "cosine,l1,l2".to_string());
    let index_column_id = 1;
    let table_index = TableIndex {
        index_type: TableIndexType::Vector,
        name: index_name.clone(),
        column_ids: vec![index_column_id],
        sync_creation: true,
        version: index_version.clone(),
        options: index_options.clone(),
    };
    let mut table_indexes = BTreeMap::new();
    table_indexes.insert("idx1".to_string(), table_index);

    // create test table
    let create_table_plan = CreateTablePlan {
        catalog: "default".to_owned(),
        create_option: CreateOption::Create,
        tenant: fixture.default_tenant(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        schema: test_schema.clone(),
        engine: Engine::Fuse,
        engine_options: Default::default(),
        storage_params: None,
        options: [
            (FUSE_OPT_KEY_ROW_PER_BLOCK.to_owned(), num_blocks_opt),
            (FUSE_OPT_KEY_BLOCK_PER_SEGMENT.to_owned(), "5".to_owned()),
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
        table_indexes: Some(table_indexes),
        table_constraints: None,
        auto_increments: BTreeMap::new(),
        attached_columns: None,
        table_partition: None,
        table_properties: None,
    };

    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // get table
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    // prepare test blocks
    let vals0: Vec<f32> = vec![
        -0.6886994,
        0.594091,
        0.90251666,
        -0.5796461,
        -0.82056284,
        0.80095357,
        0.6307791,
        -0.10274009,
        0.80994654,
        0.17736527,
        -0.65107286,
        -0.34088722,
        -0.06466371,
        -0.20792475,
        0.15237674,
        0.51079565,
        -0.6937013,
        -0.5297969,
        0.7171806,
        0.785739,
        -0.65965945,
        -0.81779677,
        0.35969305,
        -0.46954358,
        -0.8181405,
        -0.6114142,
        -0.87969273,
        -0.00383717,
        0.918081,
        -0.08214826,
        -0.2705187,
        -0.39889243,
        0.6290396,
        0.9973043,
        -0.3085359,
        0.8468473,
        -0.32078063,
        0.67827964,
        0.9978988,
        -0.30051866,
    ];
    let vals0 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals0) };
    let block0 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        Column::Vector(VectorColumn::Float32((vals0.into(), 4))),
    ]);
    let vals1: Vec<f32> = vec![
        -0.8662579,
        0.21157496,
        0.04832743,
        0.01034609,
        -0.8213428,
        -0.07455289,
        0.79567593,
        0.22692858,
        -0.13815483,
        0.04082361,
        -0.04671623,
        0.07829991,
        -0.4285805,
        -0.83638775,
        0.16173266,
        -0.6230965,
        0.48879236,
        -0.8992002,
        0.6461996,
        -0.6104055,
        0.7835251,
        0.6034467,
        0.12212521,
        0.49520096,
        0.5970688,
        0.45890963,
        -0.05623427,
        -0.49175563,
        -0.8342597,
        -0.5295784,
        0.6283545,
        0.08985507,
        -0.60963225,
        -0.9484875,
        -0.40452087,
        -0.87066746,
        0.48526454,
        0.03684357,
        0.63801855,
        -0.49714512,
    ];
    let vals1 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals1) };
    let block1 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]),
        Column::Vector(VectorColumn::Float32((vals1.into(), 4))),
    ]);
    let vals2: Vec<f32> = vec![
        -0.18905626,
        0.6927208,
        0.7869001,
        0.22925916,
        -0.5255186,
        0.14997292,
        -0.5750151,
        0.51772356,
        -0.951746,
        0.9412492,
        0.4678889,
        0.46652728,
        0.61070764,
        -0.66532606,
        -0.76100147,
        -0.12496163,
        -0.6957283,
        0.8386284,
        -0.15284961,
        -0.2555948,
        -0.22072262,
        0.42040154,
        0.99745035,
        0.6271642,
        0.9605643,
        -0.65621495,
        -0.4781119,
        0.6010602,
        0.7315234,
        -0.03415851,
        -0.12357767,
        0.09560691,
        0.21121186,
        0.2585377,
        0.5601369,
        0.23845962,
        -0.35424188,
        0.17996286,
        -0.4941602,
        -0.20577724,
    ];
    let vals2 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals2) };
    let block2 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![21, 22, 23, 24, 25, 26, 27, 28, 29, 30]),
        Column::Vector(VectorColumn::Float32((vals2.into(), 4))),
    ]);
    let vals3: Vec<f32> = vec![
        0.8412355,
        0.3082751,
        0.59870875,
        -0.54127926,
        -0.9425862,
        -0.4464907,
        -0.82330227,
        -0.33117214,
        0.13021936,
        -0.6236809,
        0.96284235,
        -0.5690468,
        -0.2858306,
        0.4726673,
        -0.1239042,
        -0.6170608,
        -0.00327663,
        -0.83231056,
        0.16952398,
        -0.01978558,
        0.6004247,
        0.09402651,
        0.9722124,
        -0.46700177,
        0.59854394,
        0.43756092,
        -0.60489684,
        -0.77390605,
        -0.33195212,
        0.20036773,
        -0.78870934,
        0.06877671,
        0.90521765,
        0.76765245,
        -0.5661686,
        -0.85996264,
        -0.8881472,
        0.7931559,
        0.2554919,
        -0.8342734,
    ];
    let vals3 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals3) };
    let block3 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![31, 32, 33, 34, 35, 36, 37, 38, 39, 40]),
        Column::Vector(VectorColumn::Float32((vals3.into(), 4))),
    ]);
    let vals4: Vec<f32> = vec![
        -0.07214834,
        -0.45140868,
        0.52644473,
        -0.9244883,
        -0.30683544,
        -0.54323095,
        -0.21925122,
        -0.12423284,
        -0.8629535,
        0.58288944,
        0.75837606,
        0.03510276,
        -0.8564059,
        -0.03417623,
        -0.07238109,
        0.58050597,
        0.7454117,
        -0.27445704,
        0.45540568,
        -0.5408085,
        -0.780661,
        0.6657731,
        -0.97462314,
        0.8857822,
        0.02701622,
        0.04349842,
        0.5408021,
        0.7438895,
        -0.44429415,
        0.77314705,
        -0.36297366,
        -0.6039303,
        0.19068193,
        0.14782214,
        0.75198305,
        -0.10257443,
        -0.08388132,
        -0.7079838,
        -0.45469823,
        0.4560124,
    ];
    let vals4 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals4) };
    let block4 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![41, 42, 43, 44, 45, 46, 47, 48, 49, 50]),
        Column::Vector(VectorColumn::Float32((vals4.into(), 4))),
    ]);
    let vals5: Vec<f32> = vec![
        -0.1999165,
        0.52322525,
        -0.337038,
        -0.90144914,
        -0.8406314,
        -0.5335526,
        -0.95726347,
        0.33673206,
        -0.8691562,
        0.48139447,
        -0.6788517,
        0.3771608,
        0.4059562,
        -0.58860403,
        -0.428289,
        0.32089558,
        -0.3011892,
        0.60242313,
        -0.87302023,
        -0.25639316,
        -0.9859232,
        0.29515472,
        0.55974996,
        -0.8190884,
        -0.08609874,
        -0.50538206,
        0.0652289,
        0.7410794,
        -0.59104115,
        0.8998315,
        0.31411764,
        0.5163839,
        0.25237387,
        0.02671343,
        -0.8648633,
        0.95094275,
        -0.6676619,
        0.62161124,
        0.6938727,
        -0.10332275,
    ];
    let vals5 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals5) };
    let block5 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![51, 52, 53, 54, 55, 56, 57, 58, 59, 60]),
        Column::Vector(VectorColumn::Float32((vals5.into(), 4))),
    ]);
    let vals6: Vec<f32> = vec![
        0.5895334,
        -0.7343663,
        -0.02117946,
        0.1402015,
        0.6598045,
        -0.722716,
        -0.40154833,
        -0.10447401,
        -0.78196186,
        0.436223,
        -0.8290139,
        0.22458494,
        -0.01400176,
        0.3236723,
        0.17722614,
        0.9377708,
        0.09351188,
        0.8986833,
        -0.8690766,
        0.10546188,
        -0.2846303,
        -0.454967,
        -0.5632622,
        0.46904188,
        -0.39408457,
        -0.1404441,
        -0.5426498,
        -0.7066665,
        0.8154848,
        0.92514247,
        -0.449755,
        0.62942183,
        0.5758866,
        0.8156669,
        -0.15692636,
        -0.15390746,
        0.457048,
        0.47833237,
        0.63010204,
        0.81386733,
    ];
    let vals6 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals6) };
    let block6 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![61, 62, 63, 64, 65, 66, 67, 68, 69, 70]),
        Column::Vector(VectorColumn::Float32((vals6.into(), 4))),
    ]);
    let vals7: Vec<f32> = vec![
        0.2055598,
        -0.9889231,
        0.48384285,
        0.6735521,
        0.42140472,
        -0.56612134,
        -0.3547931,
        0.37290242,
        -0.63698244,
        0.25703365,
        -0.6497194,
        -0.00122721,
        0.01125184,
        -0.32437629,
        -0.23926528,
        -0.13202162,
        -0.37527475,
        -0.23734985,
        0.03072986,
        -0.08610785,
        0.09782696,
        -0.05098151,
        -0.01559174,
        -0.59764004,
        -0.48390508,
        0.71857893,
        -0.4476935,
        0.6353149,
        -0.9063252,
        0.03339462,
        -0.13207407,
        0.35822904,
        0.14378202,
        -0.6895029,
        -0.45171574,
        0.7036348,
        -0.05764073,
        -0.04511834,
        -0.6025827,
        0.42203856,
    ];
    let vals7 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals7) };
    let block7 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![71, 72, 73, 74, 75, 76, 77, 78, 79, 80]),
        Column::Vector(VectorColumn::Float32((vals7.into(), 4))),
    ]);
    let vals8: Vec<f32> = vec![
        0.44271547,
        0.04186246,
        -0.05471806,
        0.84741205,
        -0.60298675,
        0.13338158,
        -0.01588953,
        0.2876288,
        -0.09086735,
        -0.11241615,
        0.03860525,
        0.63135403,
        0.60686076,
        -0.32387394,
        -0.66953754,
        0.7155654,
        -0.40972582,
        -0.70375466,
        0.28354865,
        -0.75318587,
        0.11960128,
        -0.10885316,
        0.30722642,
        0.11420934,
        -0.5221141,
        0.31499448,
        0.86042684,
        0.47856066,
        -0.82223445,
        0.7333596,
        -0.32723898,
        -0.4398808,
        0.9394175,
        -0.25679085,
        0.2887939,
        -0.73664117,
        0.5395438,
        -0.05887805,
        0.36002022,
        -0.72944045,
    ];
    let vals8 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals8) };
    let block8 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![81, 82, 83, 84, 85, 86, 87, 88, 89, 90]),
        Column::Vector(VectorColumn::Float32((vals8.into(), 4))),
    ]);
    let vals9: Vec<f32> = vec![
        0.95527714,
        -0.03856075,
        -0.89367366,
        0.90464765,
        0.7934615,
        -0.50674295,
        0.5941392,
        -0.35010257,
        0.45648512,
        -0.11480136,
        0.9441768,
        0.07530943,
        0.07846592,
        -0.15600504,
        -0.28246698,
        0.19841912,
        0.07780663,
        0.1556818,
        -0.2927237,
        0.07868534,
        0.13883874,
        -0.8788782,
        0.7045493,
        -0.23339222,
        0.95576626,
        -0.9563942,
        -0.13632946,
        0.06362384,
        0.44660464,
        0.6827207,
        0.5226848,
        -0.23891447,
        0.48967868,
        0.9801073,
        -0.5306416,
        -0.36345342,
        0.42729795,
        0.92860633,
        0.8177991,
        -0.24459854,
    ];
    let vals9 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals9) };
    let block9 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![91, 92, 93, 94, 95, 96, 97, 98, 99, 100]),
        Column::Vector(VectorColumn::Float32((vals9.into(), 4))),
    ]);

    let blocks = vec![
        block0, block1, block2, block3, block4, block5, block6, block7, block8, block9,
    ];

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // Define query vectors for testing
    let query_values1 = vec![-0.6886994, 0.594091, 0.90251667, -0.5796461];
    let query_values1 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(query_values1) };
    let query_values2 = vec![0.5758866, 0.8156669, -0.15692637, -0.15390747];
    let query_values2 = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(query_values2) };

    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    let fuse_table = FuseTable::do_create(table.get_table_info().clone())?;
    let snapshot = fuse_table.read_table_snapshot().await?;
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();

    let orderby_expr = RemoteExpr::<String>::ColumnRef {
        span: None,
        id: "_vector_score".to_string(),
        data_type: DataType::Number(NumberDataType::Float32),
        display_name: "_vector_score".to_string(),
    };

    let vector_index = VectorIndexInfo {
        index_name: index_name.clone(),
        index_version: index_version.clone(),
        index_options: index_options.clone(),
        column_id: index_column_id,
        func_name: "".to_string(),
        query_values: vec![],
    };

    let query_values = vec![
        ("cosine_distance".to_string(), query_values1.clone()),
        ("l1_distance".to_string(), query_values1.clone()),
        ("l2_distance".to_string(), query_values1.clone()),
        ("cosine_distance".to_string(), query_values2.clone()),
        ("l1_distance".to_string(), query_values2.clone()),
        ("l2_distance".to_string(), query_values2.clone()),
    ];

    let results = vec![
        // First query: cosine_distance with query_values1
        vec![
            vec![
                VectorScoreResult::new(0, 0, 0, 0.005022526),
                VectorScoreResult::new(0, 0, 9, 0.05992174),
                VectorScoreResult::new(0, 0, 1, 0.09289217),
            ],
            vec![
                VectorScoreResult::new(1, 0, 9, 0.05186367),
                VectorScoreResult::new(1, 0, 5, 0.07403374),
            ],
        ],
        // Second query: l1_distance with query_values1
        vec![
            vec![
                VectorScoreResult::new(0, 0, 0, 0.0),
                VectorScoreResult::new(0, 0, 9, 0.84269863),
                VectorScoreResult::new(0, 0, 1, 1.0792456),
            ],
            vec![VectorScoreResult::new(0, 4, 2, 0.9375271)],
            vec![VectorScoreResult::new(1, 0, 9, 0.7167929)],
        ],
        // Third query: l2_distance with query_values1
        vec![
            vec![
                VectorScoreResult::new(0, 0, 0, 3.5187712),
                VectorScoreResult::new(0, 0, 9, 3.5518785),
            ],
            vec![
                VectorScoreResult::new(1, 3, 6, 3.4702706),
                VectorScoreResult::new(1, 3, 7, 3.5206928),
                VectorScoreResult::new(1, 3, 1, 3.556445),
            ],
        ],
        // Fourth query: cosine_distance with query_values2
        vec![
            vec![VectorScoreResult::new(0, 1, 6, 0.18258381)],
            vec![VectorScoreResult::new(0, 3, 8, 0.15948296)],
            vec![
                VectorScoreResult::new(1, 1, 8, 0.008677483),
                VectorScoreResult::new(1, 1, 7, 0.21170044),
            ],
            vec![VectorScoreResult::new(1, 4, 8, 0.0657177)],
        ],
        // Fifth query: l1_distance with query_values2
        vec![
            vec![VectorScoreResult::new(0, 1, 6, 0.7965471)],
            vec![VectorScoreResult::new(0, 2, 7, 1.3045802)],
            vec![VectorScoreResult::new(1, 1, 8, 0.0)],
            vec![
                VectorScoreResult::new(1, 4, 8, 0.8538904),
                VectorScoreResult::new(1, 4, 7, 1.021619),
            ],
        ],
        // Sixth query: l2_distance with query_values2
        vec![vec![VectorScoreResult::new(1, 1, 8, 3.4763064)], vec![
            VectorScoreResult::new(1, 3, 5, 3.4903116),
            VectorScoreResult::new(1, 3, 9, 3.4926815),
            VectorScoreResult::new(1, 3, 0, 3.527872),
            VectorScoreResult::new(1, 3, 8, 3.560473),
        ]],
    ];

    let mut extras = Vec::new();
    for ((func_name, query_values), result) in
        query_values.clone().into_iter().zip(results.into_iter())
    {
        let mut vector_index = vector_index.clone();
        vector_index.func_name = func_name;
        vector_index.query_values = query_values;
        let extra = PushDownInfo {
            limit: Some(5),
            order_by: vec![(orderby_expr.clone(), true, false)],
            vector_index: Some(vector_index),
            ..Default::default()
        };
        extras.push((Some(extra), result));
    }

    // Add a filter to test filter pushdown
    let filter_arg0_expr = Expr::ColumnRef(ColumnRef {
        span: None,
        id: "_vector_score".to_string(),
        data_type: DataType::Number(NumberDataType::Float32),
        display_name: "_vector_score".to_string(),
    });
    let filter_arg1_expr = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::Float32(F32::from(2.0))),
        data_type: DataType::Number(NumberDataType::Float32),
    });
    let filter = check_function(
        None,
        "gt",
        &[],
        &[filter_arg0_expr, filter_arg1_expr],
        &BUILTIN_FUNCTIONS,
    )?;
    let inverted_filter = check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;

    let filters = Filters {
        filter: filter.as_remote_expr(),
        inverted_filter: inverted_filter.as_remote_expr(),
    };

    let filter_results = vec![
        // First query: cosine_distance with filter and query_values1
        vec![],
        // Second query: l1_distance with filter and query_values1
        vec![
            vec![
                VectorScoreResult::new(0, 0, 0, 0.0),
                VectorScoreResult::new(0, 0, 1, 1.0792456),
                VectorScoreResult::new(0, 0, 2, 3.7108307),
                VectorScoreResult::new(0, 0, 3, 3.2820892),
                VectorScoreResult::new(0, 0, 4, 2.6611536),
                VectorScoreResult::new(0, 0, 5, 2.0845702),
                VectorScoreResult::new(0, 0, 6, 3.6664782),
                VectorScoreResult::new(0, 0, 7, 3.6369097),
                VectorScoreResult::new(0, 0, 8, 4.361335),
                VectorScoreResult::new(0, 0, 9, 0.84269863),
            ],
            vec![
                VectorScoreResult::new(0, 1, 0, 1.9089664),
                VectorScoreResult::new(0, 1, 1, 1.6205615),
                VectorScoreResult::new(0, 1, 2, 2.6231122),
                VectorScoreResult::new(0, 1, 3, 2.375908),
                VectorScoreResult::new(0, 1, 4, 2.8565829),
                VectorScoreResult::new(0, 1, 5, 3.26859),
                VectorScoreResult::new(0, 1, 6, 2.3896415),
                VectorScoreResult::new(0, 1, 7, 2.11497),
                VectorScoreResult::new(0, 1, 8, 3.1175208),
                VectorScoreResult::new(0, 1, 9, 1.9913678),
            ],
            vec![
                VectorScoreResult::new(0, 2, 0, 1.5041043),
                VectorScoreResult::new(0, 2, 1, 3.1616886),
                VectorScoreResult::new(0, 2, 2, 2.0873284),
                VectorScoreResult::new(0, 2, 3, 4.6504445),
                VectorScoreResult::new(0, 2, 4, 1.6268883),
                VectorScoreResult::new(0, 2, 5, 1.9338483),
                VectorScoreResult::new(0, 2, 6, 5.4485407),
                VectorScoreResult::new(0, 2, 7, 3.7449126),
                VectorScoreResult::new(0, 2, 8, 2.3789403),
                VectorScoreResult::new(0, 2, 9, 2.5017245),
            ],
            vec![
                VectorScoreResult::new(0, 3, 0, 2.1560328),
                VectorScoreResult::new(0, 3, 1, 3.256665),
                VectorScoreResult::new(0, 3, 2, 2.0957243),
                VectorScoreResult::new(0, 3, 3, 1.5981783),
                VectorScoreResult::new(0, 3, 4, 3.4074366),
                VectorScoreResult::new(0, 3, 5, 1.9751071),
                VectorScoreResult::new(0, 3, 6, 3.151125),
                VectorScoreResult::new(0, 3, 7, 3.0908165),
                VectorScoreResult::new(0, 3, 8, 3.543131),
                VectorScoreResult::new(0, 3, 9, 1.3117124),
            ],
            vec![
                VectorScoreResult::new(1, 0, 0, 2.1198769),
                VectorScoreResult::new(1, 0, 1, 4.0567427),
                VectorScoreResult::new(1, 0, 2, 2.8214188),
                VectorScoreResult::new(1, 0, 3, 4.499019),
                VectorScoreResult::new(1, 0, 4, 2.4858987),
                VectorScoreResult::new(1, 0, 5, 1.1590694),
                VectorScoreResult::new(1, 0, 6, 3.8737319),
                VectorScoreResult::new(1, 0, 7, 2.074124),
                VectorScoreResult::new(1, 0, 8, 4.8192883),
                VectorScoreResult::new(1, 0, 9, 0.7167929),
            ],
        ],
        // Third query: l2_distance with filter and query_values1
        vec![
            vec![
                VectorScoreResult::new(0, 0, 0, 3.5187712),
                VectorScoreResult::new(0, 0, 1, 3.5688834),
                VectorScoreResult::new(0, 0, 2, 4.158467),
                VectorScoreResult::new(0, 0, 3, 3.8972623),
                VectorScoreResult::new(0, 0, 4, 3.9402654),
                VectorScoreResult::new(0, 0, 5, 3.8295133),
                VectorScoreResult::new(0, 0, 6, 4.16049),
                VectorScoreResult::new(0, 0, 7, 4.1032534),
                VectorScoreResult::new(0, 0, 8, 4.2184787),
                VectorScoreResult::new(0, 0, 9, 3.5518785),
            ],
            vec![
                VectorScoreResult::new(1, 3, 0, 3.9212408),
                VectorScoreResult::new(1, 3, 1, 3.556445),
                VectorScoreResult::new(1, 3, 2, 3.7241573),
                VectorScoreResult::new(1, 3, 3, 4.1835504),
                VectorScoreResult::new(1, 3, 4, 3.6078227),
                VectorScoreResult::new(1, 3, 5, 3.5779395),
                VectorScoreResult::new(1, 3, 6, 3.4702706),
                VectorScoreResult::new(1, 3, 7, 3.5206928),
                VectorScoreResult::new(1, 3, 8, 3.8251386),
                VectorScoreResult::new(1, 3, 9, 3.616931),
            ],
        ],
        // Fourth query: cosine_distance with filter and query_values2
        vec![],
        // Fifth query: l1_distance with filter and query_values2
        vec![
            vec![
                VectorScoreResult::new(0, 2, 0, 2.2254603),
                VectorScoreResult::new(0, 2, 1, 2.8700764),
                VectorScoreResult::new(0, 2, 2, 2.9007723),
                VectorScoreResult::new(0, 2, 3, 2.1487203),
                VectorScoreResult::new(0, 2, 4, 1.3966682),
                VectorScoreResult::new(0, 2, 5, 3.1463404),
                VectorScoreResult::new(0, 2, 6, 2.9468164),
                VectorScoreResult::new(0, 2, 7, 1.3045802),
                VectorScoreResult::new(0, 2, 8, 2.0566323),
                VectorScoreResult::new(0, 2, 9, 1.9645443),
            ],
            vec![
                VectorScoreResult::new(0, 4, 0, 3.383887),
                VectorScoreResult::new(0, 4, 1, 2.329169),
                VectorScoreResult::new(0, 4, 2, 2.7686348),
                VectorScoreResult::new(0, 4, 3, 3.0909097),
                VectorScoreResult::new(0, 4, 4, 2.2852223),
                VectorScoreResult::new(0, 4, 5, 3.3545892),
                VectorScoreResult::new(0, 4, 6, 2.9151235),
                VectorScoreResult::new(0, 4, 7, 1.7139168),
                VectorScoreResult::new(0, 4, 8, 2.0068939),
                VectorScoreResult::new(0, 4, 9, 3.0762608),
            ],
            vec![
                VectorScoreResult::new(1, 0, 0, 2.0131204),
                VectorScoreResult::new(1, 0, 1, 4.071994),
                VectorScoreResult::new(1, 0, 2, 2.8366697),
                VectorScoreResult::new(1, 0, 3, 2.3181388),
                VectorScoreResult::new(1, 0, 4, 1.921615),
                VectorScoreResult::new(1, 0, 5, 3.4619572),
                VectorScoreResult::new(1, 0, 6, 3.0959353),
                VectorScoreResult::new(1, 0, 7, 2.3943932),
                VectorScoreResult::new(1, 0, 8, 2.9434261),
                VectorScoreResult::new(1, 0, 9, 2.3486407),
            ],
            vec![
                VectorScoreResult::new(1, 3, 0, 2.011335),
                VectorScoreResult::new(1, 3, 1, 2.4690871),
                VectorScoreResult::new(1, 3, 2, 2.5800574),
                VectorScoreResult::new(1, 3, 3, 2.5523148),
                VectorScoreResult::new(1, 3, 4, 3.5649178),
                VectorScoreResult::new(1, 3, 5, 2.1223052),
                VectorScoreResult::new(1, 3, 6, 3.25975),
                VectorScoreResult::new(1, 3, 7, 1.9281074),
                VectorScoreResult::new(1, 3, 8, 2.4968297),
                VectorScoreResult::new(1, 3, 9, 2.011335),
            ],
        ],
        // Sixth query: l2_distance with filter and query_values2
        vec![
            vec![
                VectorScoreResult::new(1, 1, 0, 3.819309),
                VectorScoreResult::new(1, 1, 1, 3.8105543),
                VectorScoreResult::new(1, 1, 2, 3.8291273),
                VectorScoreResult::new(1, 1, 3, 3.7389958),
                VectorScoreResult::new(1, 1, 4, 3.5913217),
                VectorScoreResult::new(1, 1, 5, 3.870244),
                VectorScoreResult::new(1, 1, 6, 3.7941854),
                VectorScoreResult::new(1, 1, 7, 3.585766),
                VectorScoreResult::new(1, 1, 8, 3.4763064),
                VectorScoreResult::new(1, 1, 9, 3.709784),
            ],
            vec![
                VectorScoreResult::new(1, 3, 0, 3.527872),
                VectorScoreResult::new(1, 3, 1, 3.5928586),
                VectorScoreResult::new(1, 3, 2, 3.5736349),
                VectorScoreResult::new(1, 3, 3, 3.6239994),
                VectorScoreResult::new(1, 3, 4, 3.8320742),
                VectorScoreResult::new(1, 3, 5, 3.4903116),
                VectorScoreResult::new(1, 3, 6, 3.70468),
                VectorScoreResult::new(1, 3, 7, 3.586185),
                VectorScoreResult::new(1, 3, 8, 3.560473),
                VectorScoreResult::new(1, 3, 9, 3.4926815),
            ],
        ],
    ];

    for ((func_name, query_values), result) in
        query_values.into_iter().zip(filter_results.into_iter())
    {
        let mut vector_index = vector_index.clone();
        vector_index.func_name = func_name;
        vector_index.query_values = query_values;
        let extra = PushDownInfo {
            limit: Some(5),
            filters: Some(filters.clone()),
            order_by: vec![(orderby_expr.clone(), true, false)],
            vector_index: Some(vector_index),
            ..Default::default()
        };
        extras.push((Some(extra), result));
    }

    for (extra, expected_results) in extras {
        let block_metas = apply_block_pruning(
            snapshot.clone(),
            table.get_table_info().schema(),
            &extra,
            ctx.clone(),
            fuse_table.get_operator(),
            fuse_table.bloom_index_cols(),
        )
        .await?;

        assert_eq!(block_metas.len(), expected_results.len());
        for ((block_meta_index, _), expected_scores) in
            block_metas.iter().zip(expected_results.iter())
        {
            assert!(block_meta_index.vector_scores.is_some());
            let vector_scores = block_meta_index.vector_scores.clone().unwrap();
            assert_eq!(vector_scores.len(), expected_scores.len());
            for (vector_score, expected_score) in vector_scores.iter().zip(expected_scores) {
                assert_eq!(block_meta_index.segment_idx, expected_score.segment_idx);
                assert_eq!(block_meta_index.block_idx, expected_score.block_idx);
                assert_eq!(vector_score.0, expected_score.vector_idx);
                assert_eq!(vector_score.1, expected_score.score);
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct VectorScoreResult {
    segment_idx: usize,
    block_idx: usize,
    vector_idx: usize,
    score: f32,
}

impl VectorScoreResult {
    fn new(segment_idx: usize, block_idx: usize, vector_idx: usize, score: f32) -> Self {
        Self {
            segment_idx,
            block_idx,
            vector_idx,
            score,
        }
    }
}
