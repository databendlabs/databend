use std::str;

use common_dal2::services::fs;
use common_dal2::DataAccessor;
use futures::io::AsyncReadExt;
use futures::io::Cursor;

#[tokio::test]
async fn normal() {
    let f = DataAccessor::new(fs::Backend::build().finish());

    // Test write
    let x = f
        .write("/tmp/x", 13)
        .run(Box::new(Cursor::new("Hello, world!")))
        .await
        .unwrap();
    assert_eq!(13, x);

    // Test read
    let mut buf: Vec<u8> = Vec::new();
    let mut x = f.read("/tmp/x").run().await.unwrap();
    x.read_to_end(&mut buf).await.unwrap();
    assert_eq!("Hello, world!", str::from_utf8(&buf).unwrap());
}
