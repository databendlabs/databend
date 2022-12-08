use std::sync::Arc;
use time::Duration;
use opendal::Operator;
use crate::configs::Config;
use once_cell::sync::OnceCell;
use common_base::base::Singleton;
use common_exception::Result;
use common_storage::{init_operator, StorageParams};
use crate::models;
use crate::models::{PresignFileResponse, SharedTableResponse};

static SHARING_ACCESSOR: OnceCell<Singleton<Arc<SharingAccessor>>> = OnceCell::new();

#[derive(Clone)]
pub struct SharingAccessor {
    op : Arc<Operator>,
    config: Config,
}

// file would have two kind of path:
// 1. with root. e.g. /root1/root2/root3/db1/tb1/file1
// 2. without root e.g. db1/tb1/file1
// after it would be converted to file1
// and then it would use the location in table spec to form the final path
// {localtion}/file1
pub fn truncate_root(root: String, loc: String) -> String {
    let root = root.trim_matches('/');
    let loc = loc.trim_matches('/');
    return if loc.starts_with(root) {
        let o1 = loc.strip_prefix(root).unwrap();

        let updated = o1.trim_matches('/');
        let arr = updated.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        updated.to_string()
    } else {
        let arr = loc.split('/').collect::<Vec<&str>>();
        if arr.len() > 2 {
            return arr[2..].join("/");
        }
        loc.to_string()
    }
}

impl SharingAccessor {
    pub async fn init(cfg: &Config, v: Singleton<Arc<SharingAccessor>>) -> Result<()> {
        let op = init_operator(&cfg.storage.params)?;
        v.init(Arc::new(SharingAccessor {
            op:    Arc::new(op),
            config: cfg.clone(),
        }))?;

        SHARING_ACCESSOR.set(v).ok();
        Ok(())
    }
    pub fn instance() -> Arc<SharingAccessor> {
        match SHARING_ACCESSOR.get() {
            None => panic!("Sharing Accessor is not init"),
            Some(sharing_accessor) => sharing_accessor.get(),
        }
    }

    fn get_root(&self) -> String {
        match self.config.storage.params {
            StorageParams::S3(ref s3) => s3.root.trim_matches('/').to_string(),
            _ => "".to_string(),
        }
    }
    fn get_path(&self) -> String {
        format!("{}/{}", self.get_root(), self.config.tenant)
    }

    fn get_share_location(&self) -> String {
        format!("{}/_share_config/share_specs.json", self.config.tenant)
    }

    pub async fn get_shared_table(&self, input: &models::LambdaInput) -> Result<Option<SharedTableResponse>> {
        let sharing_accessor = Self::instance();
        let path = sharing_accessor.get_share_location();
        println!("path: {}", path);
        // let path = "t1/_share_config/share_specs.json";
        let data = sharing_accessor.op.object(&*path).read().await?;
        let share_specs: models::SharingConfig = serde_json::from_slice(data.as_slice())?;
        return share_specs.get_tables(input);
    }

    pub async fn presign_file(&self, table: &SharedTableResponse, input: &models::RequestFile) -> Result<PresignFileResponse> {
        let loc_prefix = table.location.trim_matches('/');
        println!("loc_prefix: {}", loc_prefix.clone());
        println!("input: {}", input.file_name);
        let file_path = truncate_root(self.get_root(), input.file_name.clone());
        let loc_prefix = loc_prefix.strip_prefix(self.get_root().as_str()).unwrap();
        let obj_path = format!("{}/{}", loc_prefix, file_path);
        let op =  self.op.clone();
        if input.method == "HEAD" {
            let s = op.object(obj_path.as_str()).presign_stat(Duration::hours(1))?;
            return Ok( PresignFileResponse::new(&s, input.file_name.clone()));
        }

        let s = op.object(obj_path.as_str()).presign_read(Duration::hours(1))?;
        return Ok( PresignFileResponse::new(&s, input.file_name.clone()));
    }

    pub async fn get_presigned_files(input: &models::LambdaInput) -> Result<Vec<PresignFileResponse>> {

        let accessor = Self::instance();
        let table = accessor.get_shared_table(input).await?;
        return match table {
            Some(t) => {
                let mut presigned_files = vec![];
                for f in input.request_files.iter() {
                    let presigned_file = accessor.presign_file(&t, f).await?;
                    presigned_files.push(presigned_file);
                }
                Ok(presigned_files)
            }
            None => {
                Ok(vec![])
            }
        }
    }
}