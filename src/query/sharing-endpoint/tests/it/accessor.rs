use common_base::base::tokio;
use common_exception::Result;
use sharing_endpoint::accessor::truncate_root;

#[tokio::test]
async fn test_truncate_root() -> Result<()> {
    let t = truncate_root("tenant1/".to_string(), "tenant1/db1/tb1/file1".to_string());
    assert_eq!(t, "file1".to_string());
    Ok(())
}
