// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_through() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::datavalues::*;
    use crate::processors::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let mut through = ThroughProcessor::create();
    through.connect_to(Arc::new(test_source.number_source_transform_for_test(2)?))?;

    let mut stream = through.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
    let expect = &UInt64Array::from(vec![0, 1]);
    assert_eq!(expect.clone(), actual.clone());
    Ok(())
}
