use std::str;

use common_dal2::services::fs;
use common_dal2::DataAccessor;
use futures::io::AsyncReadExt;

#[tokio::test]
async fn read() {
    let f = DataAccessor::new(fs::Backend::build().finish());
    let mut buf: Vec<u8> = Vec::new();

    let mut x = f.read("/tmp/x").offset(100).run().await.unwrap();
    x.read_to_end(&mut buf).await.unwrap();
    println!("{}", str::from_utf8(&buf).unwrap());
}
