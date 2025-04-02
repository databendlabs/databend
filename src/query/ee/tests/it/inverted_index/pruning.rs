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
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::F32;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_sql::BloomIndexColumns;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::pruning::FusePruner;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
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

    FusePruner::create(&ctx, dal, schema, push_down, bloom_index_cols, None)?
        .read_pruning(segment_locs)
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_block_pruner() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    let ctx = fixture.new_query_ctx().await?;
    fixture.create_default_database().await?;

    let test_tbl_name = "test_index_helper";
    let test_schema = TableSchemaRefExt::create(vec![
        TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("idiom", TableDataType::String),
        TableField::new("meaning", TableDataType::String),
        TableField::new("extras", TableDataType::Variant),
    ]);

    let row_per_block = 5;
    let num_blocks_opt = row_per_block.to_string();

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
        inverted_indexes: None,
        attached_columns: None,
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
    let block0 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1, 2, 3, 4, 5]),
        StringType::from_data(vec![
            "A bird in the hand is worth two in the bush".to_string(),
            "A penny for your thoughts".to_string(),
            "A penny saved is a penny earned".to_string(),
            "A perfect storm".to_string(),
            "A picture is worth 1000 words".to_string(),
        ]),
        StringType::from_data(vec![
            "What you have is worth more than what you might have later".to_string(),
            "Tell me what you're thinking".to_string(),
            "Money you save today you can spend later".to_string(),
            "The worst possible situation".to_string(),
            "Better to show than tell".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Introduction to Full-Text Search","metadata":{"author":"John","publishedDate":"2023-04-01","tags":["database","search","indexing"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Art of Programming","metadata":{"author":"Alice","publishedDate":"2022-01-01","tags":["programming","algorithms","best practices"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Data Structures for Beginners","metadata":{"author":"Bob","publishedDate":"2021-12-15","tags":["data structures","beginner","tutorials"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Web Development Trends in 2023","metadata":{"author":"Charlie","publishedDate":"2023-02-20","tags":["web development","trends","2023"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Introduction to Databases","metadata":{"author":"David","publishedDate":"2020-09-01","tags":["databases","SQL","introductory"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block1 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![6, 7, 8, 9, 10]),
        StringType::from_data(vec![
            "Actions speak louder than words".to_string(),
            "Add insult to injury".to_string(),
            "Barking up the wrong tree".to_string(),
            "Birds of a feather flock together".to_string(),
            "Bite off more than you can chew".to_string(),
        ]),
        StringType::from_data(vec![
            "Believe what people do and not what they say".to_string(),
            "To make a bad situation worse".to_string(),
            "To be mistaken, to be looking for solutions in the wrong place".to_string(),
            "People who are alike are often friends".to_string(),
            "Take on a project that you cannot finish".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"The Future of Artificial Intelligence","metadata":{"author":"Emily","publishedDate":"2023-03-10","tags":["AI","future","predictions"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Software Architecture Patterns","metadata":{"author":"Frank","publishedDate":"2021-08-15","tags":["software architecture","patterns","design"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Front-End Development Best Practices","metadata":{"author":"Grace","publishedDate":"2022-06-01","tags":["front-end","development","best practices"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Cybersecurity Challenges in the Modern World","metadata":{"author":"Henry","publishedDate":"2023-01-05","tags":["cybersecurity","challenges","modern world"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Game Development for Kids","metadata":{"author":"Isabella","publishedDate":"2022-07-20","tags":["game development","kids","educational"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block2 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![11, 12, 13, 14, 15]),
        StringType::from_data(vec![
            "Break the ice".to_string(),
            "By the skin of your teeth".to_string(),
            "Comparing apples to oranges".to_string(),
            "Costs an arm and a leg".to_string(),
            "Do something at the drop of a hat".to_string(),
        ]),
        StringType::from_data(vec![
            "Make people feel more comfortable".to_string(),
            "Just barely".to_string(),
            "Comparing two things that cannot be compared".to_string(),
            "Very expensive".to_string(),
            "Do something without having planned beforehand".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Blockchain Technology Explained","metadata":{"author":"Jack","publishedDate":"2021-05-10","tags":["blockchain","technology","explained"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"UX Design Principles","metadata":{"author":"Alice","publishedDate":"2022-04-01","tags":["UX","design","principles"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Python for Data Analysis","metadata":{"author":"Bob","publishedDate":"2021-06-15","tags":["python","data","analysis"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Cloud Computing Fundamentals","metadata":{"author":"Charlie","publishedDate":"2023-01-10","tags":["cloud","computing","fundamentals"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"IoT Security Challenges","metadata":{"author":"David","publishedDate":"2022-09-01","tags":["IoT","security","challenges"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block3 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![16, 17, 18, 19, 20]),
        StringType::from_data(vec![
            "Do unto others as you would have them do unto you".to_string(),
            "Don't count your chickens before they hatch".to_string(),
            "Don't cry over spilt milk".to_string(),
            "Don't give up your day job".to_string(),
            "Don't put all your eggs in one basket".to_string(),
        ]),
        StringType::from_data(vec![
            "Treat people fairly. Also known as 'The Golden Rule'".to_string(),
            "Don't count on something good happening until it's happened".to_string(),
            "There's no reason to complain about something that can't be fixed".to_string(),
            "You're not very good at this".to_string(),
            "What you're doing is too risky".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Mobile App Development Guide","metadata":{"author":"Frank","publishedDate":"2021-11-15","tags":["mobile","apps","development"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Big Data Analytics in Healthcare","metadata":{"author":"Grace","publishedDate":"2023-02-10","tags":["big data","analytics","healthcare"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Digital Marketing Strategies","metadata":{"author":"Henry","publishedDate":"2022-07-05","tags":["digital","marketing","strategies"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Introduction to Quantum Computing","metadata":{"author":"Isabella","publishedDate":"2021-10-20","tags":["quantum","computing","introduction"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Software Development Methodologies","metadata":{"author":"Karen","publishedDate":"2021-08-01","tags":["software","development","methodologies"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block4 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![21, 22, 23, 24, 25]),
        StringType::from_data(vec![
            "Every cloud has a silver lining".to_string(),
            "Get a taste of your own medicine".to_string(),
            "Give someone the cold shoulder".to_string(),
            "Go on a wild goose chase".to_string(),
            "Good things come to those who wait".to_string(),
        ]),
        StringType::from_data(vec![
            "Good things come after bad things".to_string(),
            "Get treated the way you've been treating others".to_string(),
            "Ignore someone".to_string(),
            "To do something pointless".to_string(),
            "Be patient".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Machine Learning for Beginners","metadata":{"author":"Leo","publishedDate":"2023-03-15","tags":["machine learning","beginners","intro"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Responsive Web Design Techniques","metadata":{"author":"Mike","publishedDate":"2022-01-20","tags":["responsive","web","design"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Artificial Intelligence Ethics","metadata":{"author":"Nina","publishedDate":"2021-12-01","tags":["AI","ethics","morality"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Cryptocurrency Investment Guide","metadata":{"author":"Oliver","publishedDate":"2023-04-10","tags":["cryptocurrency","investment","guide"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"API Development and Integration","metadata":{"author":"Paula","publishedDate":"2022-06-15","tags":["API","development","integration"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block5 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![26, 27, 28, 29, 30]),
        StringType::from_data(vec![
            "He has bigger fish to fry".to_string(),
            "He's a chip off the old block".to_string(),
            "Hit the nail on the head".to_string(),
            "Ignorance is bliss".to_string(),
            "It ain't over till the fat lady sings".to_string(),
        ]),
        StringType::from_data(vec![
            "He has bigger things to take care of than what we are talking about now".to_string(),
            "The son is like the father".to_string(),
            "Get something exactly right".to_string(),
            "You're better off not knowing".to_string(),
            "This isn't over yet".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"The Future of Electric Vehicles","metadata":{"author":"Quincy","publishedDate":"2021-09-01","tags":["electric vehicles","future","technology"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Full-Stack Development Trends","metadata":{"author":"Emily","publishedDate":"2022-02-20","tags":["full-stack","development","trends"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Introduction to Augmented Reality","metadata":{"author":"Rachel","publishedDate":"2023-01-05","tags":["augmented reality","introduction","VR"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"E-Commerce Best Practices for SMBs","metadata":{"author":"Sam","publishedDate":"2022-08-20","tags":["e-commerce","SMBs"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"SEO Strategies for Websites","metadata":{"author":"Tina","publishedDate":"2022-10-10","tags":["SEO","websites","strategies"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block6 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![31, 32, 33, 34, 35]),
        StringType::from_data(vec![
            "It takes one to know one".to_string(),
            "It's a piece of cake".to_string(),
            "It's raining cats and dogs".to_string(),
            "Kill two birds with one stone".to_string(),
            "Let the cat out of the bag".to_string(),
        ]),
        StringType::from_data(vec![
            "You're just as bad as I am".to_string(),
            "It's easy".to_string(),
            "It's raining hard".to_string(),
            "Get two things done with a single action".to_string(),
            "Give away a secret".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Blockchain Technology Applications","metadata":{"author":"Uma","publishedDate":"2021-07-15","tags":["blockchain","technology","applications"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Data Visualization Techniques","metadata":{"author":"Victor","publishedDate":"2023-05-01","tags":["data","visualization","techniques"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Green Computing Principles","metadata":{"author":"Wendy","publishedDate":"2022-03-15","tags":["green computing","principles","sustainability"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Cybersecurity for Small Businesses","metadata":{"author":"Xavier","publishedDate":"2021-12-15","tags":["cybersecurity","small businesses","security"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Artificial Intelligence in Education","metadata":{"author":"Yvonne","publishedDate":"2023-06-10","tags":["AI","education","applications"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block7 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![36, 37, 38, 39, 40]),
        StringType::from_data(vec![
            "Live and learn".to_string(),
            "Look before you leap".to_string(),
            "On thin ice".to_string(),
            "Once in a blue moon".to_string(),
            "Play devil's advocate".to_string(),
        ]),
        StringType::from_data(vec![
            "I made a mistake".to_string(),
            "Take only calculated risks".to_string(),
            "On probation. If you make another mistake, there will be trouble".to_string(),
            "Rarely".to_string(),
            "To argue the opposite, just for the sake of argument".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Social Media Marketing Trends","metadata":{"author":"Zach","publishedDate":"2022-11-05","tags":["social media","marketing","trends"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Fundamentals of Cryptography","metadata":{"author":"Amy","publishedDate":"2021-04-20","tags":["cryptography","fundamentals","security"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Game Development for Beginners","metadata":{"author":"Beth","publishedDate":"2023-07-15","tags":["game development","beginners","programming"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"FinTech Innovations in Banking","metadata":{"author":"Carl","publishedDate":"2022-02-10","tags":["FinTech","banking","innovations"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Power of Content Marketing","metadata":{"author":"Diana","publishedDate":"2021-11-01","tags":["content marketing","strategy","advertising"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block8 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![41, 42, 43, 44, 45]),
        StringType::from_data(vec![
            "Put something on ice".to_string(),
            "Rain on someone's parade".to_string(),
            "Saving for a rainy day".to_string(),
            "Slow and steady wins the race".to_string(),
            "Spill the beans".to_string(),
        ]),
        StringType::from_data(vec![
            "Put a projet on hold".to_string(),
            "To spoil something".to_string(),
            "Saving money for later".to_string(),
            "Reliability is more important than speed".to_string(),
            "Give away a secret".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"Web Development Frameworks","metadata":{"author":"Edwin","publishedDate":"2023-08-05","tags":["web development","frameworks","technology"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Economics of Cryptocurrency","metadata":{"author":"Frankie","publishedDate":"2022-09-15","tags":["cryptocurrency","economics","finance"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Rise of 5G Technology","metadata":{"author":"Georgia","publishedDate":"2021-05-01","tags":["5G","technology","communications"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Introduction to Quantum Computing","metadata":{"author":"Hannah","publishedDate":"2023-09-10","tags":["quantum computing","introduction","physics"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Future of Remote Work","metadata":{"author":"Ian","publishedDate":"2022-04-15","tags":["remote work","future","trends"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block9 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![46, 47, 48, 49, 50]),
        StringType::from_data(vec![
            "Take a rain check".to_string(),
            "Take it with a grain of salt".to_string(),
            "The ball is in your court".to_string(),
            "The best thing since sliced bread".to_string(),
            "The devil is in the details".to_string(),
        ]),
        StringType::from_data(vec![
            "Postpone a plan".to_string(),
            "Don’t take it too seriously".to_string(),
            "It's your decision".to_string(),
            "A really good invention".to_string(),
            "It looks good from a distance, but when you look closer, there are problems"
                .to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"The Principles of Agile Development","metadata":{"author":"Jared","publishedDate":"2021-10-10","tags":["agile","development","principles"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"UI/UX Design for Mobile Apps","metadata":{"author":"Katherine","publishedDate":"2023-10-05","tags":["UI/UX","mobile apps","design"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Fundamentals of Data Science","metadata":{"author":"Larry","publishedDate":"2022-07-01","tags":["data","science"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Big Data Analytics in Healthcare","metadata":{"author":"Mia","publishedDate":"2023-11-15","tags":["big data","analytics","healthcare"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Cloud Computing for Startups","metadata":{"author":"Noah","publishedDate":"2022-08-10","tags":["cloud computing","startups","technology"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block10 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![51, 52, 53, 54, 55]),
        StringType::from_data(vec![
            "The early bird gets the worm".to_string(),
            "The elephant in the room".to_string(),
            "The whole nine yards".to_string(),
            "There are other fish in the sea".to_string(),
            "There's a method to his madness".to_string(),
        ]),
        StringType::from_data(vec![
            "The first people who arrive will get the best stuff".to_string(),
            "The big issue, the problem people are avoiding".to_string(),
            "Everything, all the way".to_string(),
            "It's ok to miss this opportunity. Others will arise".to_string(),
            "He seems crazy but actually he's clever".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"The Psychology of Persuasion","metadata":{"author":"Oliver","publishedDate":"2021-06-15","tags":["psychology","persuasion","behavior"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Sustainable Energy Solutions","metadata":{"author":"Pamela","publishedDate":"2023-12-01","tags":["sustainable energy","solutions","environment"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Future of Autonomous Vehicles","metadata":{"author":"Quincy","publishedDate":"2022-05-05","tags":["autonomous vehicles","future","technology"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Role of AI in Customer Service","metadata":{"author":"Rachel","publishedDate":"2021-09-20","tags":["AI","customer service","automation"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Internet of Things Applications","metadata":{"author":"Samuel","publishedDate":"2023-12-15","tags":["IoT","applications","technology"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block11 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![56, 57, 58, 59, 60]),
        StringType::from_data(vec![
            "There's no such thing as a free lunch".to_string(),
            "Throw caution to the wind".to_string(),
            "You can't have your cake and eat it too".to_string(),
            "You can't judge a book by its cover".to_string(),
            "A little learning is a dangerous thing".to_string(),
        ]),
        StringType::from_data(vec![
            "Nothing is entirely free".to_string(),
            "Take a risk".to_string(),
            "You can't have everything".to_string(),
            "This person or thing may look bad, but it's good inside".to_string(),
            "People who don't understand something fully are dangerous".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"The Art of Storytelling in Marketing","metadata":{"author":"Thomas","publishedDate":"2022-06-10","tags":["storytelling","marketing","advertising"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Innovations in Renewable Energy","metadata":{"author":"Ursula","publishedDate":"2021-08-15","tags":["renewable energy","innovations","sustainability"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Impact of Social Media on Society","metadata":{"author":"Victoria","publishedDate":"2023-11-01","tags":["social media","impact","society"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"Virtual Reality in Tourism","metadata":{"author":"Emma","publishedDate":"2023-06-30","tags":["virtual reality","tourism","travel"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"The Future of Blockchain Technology","metadata":{"author":"Bob","publishedDate":"2023-10-01","tags":["blockchain","future","cryptocurrency"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block12 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![61, 62, 63, 64, 65]),
        StringType::from_data(vec![
            "山重水复疑无路，柳暗花明又一村".to_string(),
            "两岸猿声啼不住，轻舟已过万重山".to_string(),
            "一寸光阴一寸金，寸金难买寸光阴".to_string(),
            "人生得意须尽欢，莫使金樽空对月".to_string(),
            "天生我材必有用，千金散尽还复来".to_string(),
        ]),
        StringType::from_data(vec![
            "遇到困难一种办法不行时，可以用另一种办法去解决，通过探索去发现答案".to_string(),
            "猿猴的啼声还回荡在耳边时，轻快的小船已驶过连绵不绝的万重山峦".to_string(),
            "一寸光阴和一寸长的黄金一样昂贵，而一寸长的黄金却难以买到一寸光阴。比喻时间十分宝贵"
                .to_string(),
            "人活在世上就要尽情的享受欢乐，不要使自己的酒杯只对着月亮".to_string(),
            "上天造就了我的才干就必然是有用处的，千两黄金花完了也能够再次获得".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"人工智能与机器学习","metadata":{"author":"张三","publishedDate":"2023-10-23","tags":["人工智能","机器学习","技术"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"区块链在金融行业的应用","metadata":{"author":"李四","publishedDate":"2023-09-18","tags":["区块链","金融行业","金融科技"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"物联网与智能家居","metadata":{"author":"王五","publishedDate":"2023-08-15","tags":["物联网","智能家居","生活"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"量子计算的未来","metadata":{"author":"赵六","publishedDate":"2023-07-20","tags":["量子计算","未来科技","物理学"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"网络安全与隐私保护","metadata":{"author":"刘七","publishedDate":"2023-06-25","tags":["网络安全","隐私保护","信息技术"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);
    let block13 = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![66, 67, 68, 69, 70]),
        StringType::from_data(vec![
            "光阴似箭，岁月如梭".to_string(),
            "塞翁失马，焉知非福".to_string(),
            "绳锯木断，水滴石穿".to_string(),
            "机不可失，时不再来".to_string(),
            "一箭双雕，一举两得".to_string(),
        ]),
        StringType::from_data(vec![
            "光阴的流逝就像是射出去的箭一样，岁月如同纺织机上梭的速度一样。比喻时间流逝得非常快"
                .to_string(),
            "一时虽然受到损失，也许反而因此能得到好处，坏事在一定条件下可变为好事".to_string(),
            "用绳子也能把木头锯断，水珠滴落，天长日久也可以把石头滴穿".to_string(),
            "时机难得，必需抓紧，不可错过".to_string(),
            "一支箭射中两只雕，比喻做一件事而达到两个目的".to_string(),
        ]),
        VariantType::from_data(vec![
            jsonb::parse_value(r#"{"title":"虚拟现实在教育中的应用","metadata":{"author":"周八","publishedDate":"2023-05-30","tags":["虚拟现实","教育技术","创新教育"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"大数据与医疗健康","metadata":{"author":"吴九","publishedDate":"2023-04-25","tags":["大数据","医疗健康","数据分析"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"云计算在企业中的应用","metadata":{"author":"郑十","publishedDate":"2023-03-20","tags":["云计算","企业管理","信息技术"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"科技前沿探索","metadata":{"author":"陈十一","publishedDate":"2023-10-27","tags":["科技创新","前沿技术","探索之旅"]}}"#.as_bytes()).unwrap().to_vec(),
            jsonb::parse_value(r#"{"title":"环保生活实践","metadata":{"author":"韩十二","publishedDate":"2023-10-23","tags":["环境保护","绿色生活","可持续发展"]}}"#.as_bytes()).unwrap().to_vec(),
        ]),
    ]);

    let blocks = vec![
        block0, block1, block2, block3, block4, block5, block6, block7, block8, block9, block10,
        block11, block12, block13,
    ];

    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let table = catalog
        .get_table(
            &fixture.default_tenant(),
            fixture.default_db_name().as_str(),
            test_tbl_name,
        )
        .await?;

    // create inverted index on table
    let handler = get_inverted_index_handler();

    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let table_id = table.get_id();
    let index_name = "idx1".to_string();
    let mut index_options = BTreeMap::new();
    index_options.insert("tokenizer".to_string(), "chinese".to_string());
    index_options.insert(
        "filters".to_string(),
        "english_stop,english_stemmer,chinese_stop".to_string(),
    );
    let tenant = ctx.get_tenant();

    let req = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant,
        name: index_name.clone(),
        column_ids: vec![1, 2, 3],
        sync_creation: false,
        options: index_options.clone(),
    };

    let res = handler.do_create_table_index(catalog.clone(), req).await;
    assert!(res.is_ok());

    let index_table_schema = TableSchemaRefExt::create(vec![
        TableField::new("idiom", TableDataType::String),
        TableField::new("meaning", TableDataType::String),
        TableField::new("extras", TableDataType::Variant),
    ]);

    let refresh_index_plan = RefreshTableIndexPlan {
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: test_tbl_name.to_string(),
        index_name: index_name.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let new_table = table.refresh(ctx.as_ref()).await?;
    let fuse_table = FuseTable::do_create(new_table.get_table_info().clone())?;

    let snapshot = fuse_table.read_table_snapshot().await?;
    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();

    let table_info = new_table.get_table_info();
    let table_indexes = &table_info.meta.indexes;
    let table_index = table_indexes.get(&index_name);
    assert!(table_index.is_some());
    let table_index = table_index.unwrap();
    let index_version = table_index.version.clone();

    let index_schema = DataSchema::from(index_table_schema);
    let e1 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "test".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e2 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "save".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e3 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "one".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e4 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "the".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e5 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "光阴".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e6 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("idiom".to_string(), None)],
            query_text: "人生".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e7 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("meaning".to_string(), None)],
            query_text: "people".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e8 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("meaning".to_string(), None)],
            query_text: "bad".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e9 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("meaning".to_string(), None)],
            query_text: "黄金".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e10 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("meaning".to_string(), None)],
            query_text: "时间".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e11 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![
                ("idiom".to_string(), Some(F32::from(5.0))),
                ("meaning".to_string(), Some(F32::from(1.0))),
            ],
            query_text: "you".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e12 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![
                ("idiom".to_string(), Some(F32::from(5.0))),
                ("meaning".to_string(), Some(F32::from(1.0))),
            ],
            query_text: "光阴".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e13 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("extras".to_string(), None)],
            query_text: "extras.title:Blockchain".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e14 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("extras".to_string(), None)],
            query_text: "extras.metadata.author:David".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let e15 = PushDownInfo {
        inverted_index: Some(InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: index_options.clone(),
            index_schema: index_schema.clone(),
            query_fields: vec![("extras".to_string(), None)],
            query_text: "extras.metadata.tags:技术".to_string(),
            has_score: false,
            inverted_index_option: None,
        }),
        ..Default::default()
    };
    let extras = vec![
        (Some(e1), 0, 0),
        (Some(e2), 2, 2),
        (Some(e3), 2, 3),
        (Some(e4), 0, 0),
        (Some(e5), 2, 2),
        (Some(e6), 1, 1),
        (Some(e7), 5, 7),
        (Some(e8), 4, 4),
        (Some(e9), 1, 2),
        (Some(e10), 2, 2),
        (Some(e11), 9, 15),
        (Some(e12), 2, 2),
        (Some(e13), 3, 3),
        (Some(e14), 2, 2),
        (Some(e15), 2, 5),
    ];

    for (extra, expected_blocks, expected_rows) in extras {
        let block_metas = apply_block_pruning(
            snapshot.clone(),
            table.get_table_info().schema(),
            &extra,
            ctx.clone(),
            fuse_table.get_operator(),
            fuse_table.bloom_index_cols(),
        )
        .await?;

        let rows = block_metas
            .iter()
            .map(|(v, _)| match &v.matched_rows {
                Some(matched_rows) => matched_rows.len(),
                None => 0,
            })
            .sum::<usize>();

        assert_eq!(expected_rows, rows);
        assert_eq!(expected_blocks, block_metas.len());
    }

    Ok(())
}
