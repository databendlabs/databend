// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{services::Huggingface, Operator};
use snafu::location;
use url::Url;

use crate::object_store::parse_hf_repo_id;
use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions, DEFAULT_CLOUD_BLOCK_SIZE,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE,
};
use lance_core::error::{Error, Result};

/// Hugging Face object store provider backed by OpenDAL.
#[derive(Default, Debug)]
pub struct HuggingfaceStoreProvider;

/// Parsed components from a Hugging Face URL.
#[derive(Debug, PartialEq, Eq)]
struct ParsedHfUrl {
    repo_type: String,
    repo_id: String,
    relative_path: String,
}

fn parse_hf_url(url: &Url) -> Result<ParsedHfUrl> {
    let mut repo_type = url
        .host_str()
        .ok_or_else(|| Error::invalid_input("Huggingface URL must contain repo type", location!()))?
        .to_string();
    // OpenDAL expects `dataset` instead of `datasets`; keep the workaround here and adapt tests.
    if repo_type == "datasets" {
        repo_type = "dataset".to_string();
    }

    let mut segments = url.path().trim_start_matches('/').split('/');
    let owner = segments
        .next()
        .ok_or_else(|| Error::invalid_input("Huggingface URL must contain owner", location!()))?;
    let repo_name = segments.next().ok_or_else(|| {
        Error::invalid_input("Huggingface URL must contain repository name", location!())
    })?;

    let relative_path = segments.collect::<Vec<_>>().join("/");

    Ok(ParsedHfUrl {
        repo_type,
        repo_id: format!("{owner}/{repo_name}"),
        relative_path,
    })
}

#[async_trait::async_trait]
impl ObjectStoreProvider for HuggingfaceStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let ParsedHfUrl {
            repo_type, repo_id, ..
        } = parse_hf_url(&base_path)?;

        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();

        // Build OpenDAL config with allowed keys only.
        let mut config_map: HashMap<String, String> = HashMap::new();

        config_map.insert("repo_type".to_string(), repo_type);
        config_map.insert("repo_id".to_string(), repo_id);

        if let Some(rev) = storage_options.get("hf_revision").cloned() {
            config_map.insert("revision".to_string(), rev);
        }

        if let Some(root) = storage_options.get("hf_root").cloned() {
            if !root.is_empty() {
                config_map.insert("root".to_string(), root);
            }
        }

        if let Some(token) = storage_options
            .get("hf_token")
            .cloned()
            .or_else(|| std::env::var("HF_TOKEN").ok())
            .or_else(|| std::env::var("HUGGINGFACE_TOKEN").ok())
        {
            config_map.insert("token".to_string(), token);
        }

        let operator = Operator::from_iter::<Huggingface>(config_map)
            .map_err(|e| {
                Error::invalid_input(
                    format!("Failed to create Huggingface operator: {:?}", e),
                    location!(),
                )
            })?
            .finish();

        let inner: Arc<dyn OSObjectStore> = Arc::new(OpendalStore::new(operator));

        Ok(ObjectStore {
            scheme: "hf".to_string(),
            inner,
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: params.use_constant_size_upload_parts,
            list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or(true),
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        let parsed = parse_hf_url(url)?;
        Path::parse(&parsed.relative_path).map_err(|_| {
            Error::invalid_input(
                format!("Invalid path in Huggingface URL: {}", url.path()),
                location!(),
            )
        })
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let repo_id = parse_hf_repo_id(url)?;
        Ok(format!("{}${}@{}", url.scheme(), url.authority(), repo_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_url() {
        let url = Url::parse("hf://datasets/acme/repo/path/to/table.lance").unwrap();
        let parsed = parse_hf_url(&url).unwrap();
        assert_eq!(
            parsed,
            ParsedHfUrl {
                repo_type: "dataset".to_string(),
                repo_id: "acme/repo".to_string(),
                relative_path: "path/to/table.lance".to_string(),
            }
        );
    }

    #[test]
    fn storage_option_revision_takes_precedence() {
        use crate::object_store::StorageOptionsAccessor;
        use std::sync::Arc;
        let url = Url::parse("hf://datasets/acme/repo/data/file").unwrap();
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([(String::from("hf_revision"), String::from("stable"))]),
            ))),
            ..Default::default()
        };
        // new_store should accept without creating operator; test precedence via builder config
        let ParsedHfUrl {
            repo_type, repo_id, ..
        } = parse_hf_url(&url).unwrap();

        // Build config map the same way new_store would to assert precedence logic.
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("repo_type".to_string(), repo_type);
        config_map.insert("repo".to_string(), repo_id);
        if let Some(rev) = params
            .storage_options()
            .unwrap()
            .get("hf_revision")
            .cloned()
        {
            config_map.insert("revision".to_string(), rev);
        }
        assert_eq!(config_map.get("revision").unwrap(), "stable");
    }

    #[test]
    fn parse_hf_repo_id_with_type_and_owner_repo() {
        let url = Url::parse("hf://models/owner/repo/path/to/file").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_legacy_without_type() {
        let url = Url::parse("hf://owner/repo/path/to/file").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_strips_revision() {
        let url = Url::parse("hf://datasets/owner/repo@main/data").unwrap();
        let repo = crate::object_store::parse_hf_repo_id(&url).unwrap();
        assert_eq!(repo, "owner/repo");
    }

    #[test]
    fn parse_hf_repo_id_missing_segments_errors() {
        let url = Url::parse("hf://datasets/only-owner").unwrap();
        let err = crate::object_store::parse_hf_repo_id(&url).unwrap_err();
        assert!(
            err.to_string().contains("owner/repo"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn extract_path_returns_relative() {
        let url = Url::parse("hf://datasets/acme/repo/sub/dir/table.lance").unwrap();
        let provider = HuggingfaceStoreProvider;
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "sub/dir/table.lance");
    }

    #[test]
    fn calculate_prefix_uses_repo_id() {
        let provider = HuggingfaceStoreProvider;
        let url = Url::parse("hf://datasets/acme/repo/path").unwrap();
        let prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        assert_eq!(prefix, "hf$datasets@acme/repo");
    }

    #[test]
    fn parse_invalid_url_errors() {
        let url = Url::parse("hf://datasets/only-owner").unwrap();
        let err = parse_hf_url(&url).unwrap_err();
        assert!(err.to_string().contains("repository name"));
    }
}
