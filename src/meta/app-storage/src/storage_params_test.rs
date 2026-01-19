// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str::FromStr;

use crate::StorageAzblobConfig;
use crate::StorageCosConfig;
use crate::StorageFsConfig;
use crate::StorageFtpConfig;
use crate::StorageGcsConfig;
use crate::StorageHdfsConfig;
use crate::StorageHttpConfig;
use crate::StorageHuggingfaceConfig;
use crate::StorageIpfsConfig;
use crate::StorageMokaConfig;
use crate::StorageObsConfig;
use crate::StorageOssConfig;
use crate::StorageParams;
use crate::StorageS3Config;
use crate::StorageWebhdfsConfig;
use crate::mask_string;
use crate::storage_params::S3StorageClass;

#[test]
fn test_s3_storage_class_from_str_valid() {
    // Test valid standard case
    assert_eq!(
        S3StorageClass::from_str("STANDARD").unwrap(),
        S3StorageClass::Standard
    );
    assert_eq!(
        S3StorageClass::from_str("standard").unwrap(),
        S3StorageClass::Standard
    );
    assert_eq!(
        S3StorageClass::from_str("Standard").unwrap(),
        S3StorageClass::Standard
    );

    // Test valid intelligent tiering case
    assert_eq!(
        S3StorageClass::from_str("INTELLIGENT_TIERING").unwrap(),
        S3StorageClass::IntelligentTiering
    );
    assert_eq!(
        S3StorageClass::from_str("intelligent_tiering").unwrap(),
        S3StorageClass::IntelligentTiering
    );
    assert_eq!(
        S3StorageClass::from_str("Intelligent_Tiering").unwrap(),
        S3StorageClass::IntelligentTiering
    );
}

#[test]
fn test_s3_storage_class_from_str_invalid() {
    // Test invalid cases
    let invalid_cases = vec![
        "",
        "invalid",
        "GLACIER",
        "DEEP_ARCHIVE",
        "STANDARD_IA",
        "ONEZONE_IA",
        "REDUCED_REDUNDANCY",
        "STANDARD_INTELLIGENT_TIERING", // typo
        "INTELLIGENT-TIERING",          // wrong separator
    ];

    for invalid_case in invalid_cases {
        let result = S3StorageClass::from_str(invalid_case);
        assert!(
            result.is_err(),
            "Expected error for input: {}",
            invalid_case
        );
        assert!(
            result.unwrap_err().contains("Unsupported S3 storage class"),
            "Error message should contain 'Unsupported S3 storage class' for input: {}",
            invalid_case
        );
    }
}

#[test]
fn test_s3_storage_class_display() {
    assert_eq!(S3StorageClass::Standard.to_string(), "STANDARD");
    assert_eq!(
        S3StorageClass::IntelligentTiering.to_string(),
        "INTELLIGENT_TIERING"
    );
}

#[test]
fn test_s3_storage_class_default() {
    assert_eq!(S3StorageClass::default(), S3StorageClass::Standard);
}

#[test]
fn test_s3_storage_class_round_trip() {
    // Test that parsing and formatting are consistent
    let classes = vec![S3StorageClass::Standard, S3StorageClass::IntelligentTiering];

    for class in classes {
        let serialized = class.to_string();
        let deserialized = S3StorageClass::from_str(&serialized).unwrap();
        assert_eq!(class, deserialized);
    }
}

#[test]
fn test_bucket_style_url() {
    let s3 = StorageS3Config {
        bucket: "s3-bucket".to_string(),
        root: "/data/".to_string(),
        ..Default::default()
    };

    let gcs = StorageGcsConfig {
        bucket: "gcs-bucket".to_string(),
        root: "/stage/".to_string(),
        ..Default::default()
    };

    let oss = StorageOssConfig {
        bucket: "oss-bucket".to_string(),
        root: "/stage/".to_string(),
        ..Default::default()
    };

    let obs = StorageObsConfig {
        bucket: "obs-bucket".to_string(),
        root: "/stage/".to_string(),
        ..Default::default()
    };

    let cos = StorageCosConfig {
        bucket: "cos-bucket".to_string(),
        root: "/stage/".to_string(),
        ..Default::default()
    };

    let azblob = StorageAzblobConfig {
        container: "az-container".to_string(),
        root: "/stage/".to_string(),
        ..Default::default()
    };

    let cases = vec![
        ("s3://s3-bucket/data/", StorageParams::S3(s3)),
        ("gcs://gcs-bucket/stage/", StorageParams::Gcs(gcs)),
        ("oss://oss-bucket/stage/", StorageParams::Oss(oss)),
        ("obs://obs-bucket/stage/", StorageParams::Obs(obs)),
        ("cos://cos-bucket/stage/", StorageParams::Cos(cos)),
        (
            "azblob://az-container/stage/",
            StorageParams::Azblob(azblob),
        ),
    ];

    for (expected, params) in cases {
        assert_eq!(params.url().as_deref(), Some(expected));
    }
}

#[test]
fn test_fs_relative_url() {
    let params = StorageParams::Fs(StorageFsConfig {
        root: "tmp/data".to_string(),
    });
    assert_eq!(params.url().as_deref(), Some("fs://tmp/data/"));
}

#[test]
fn test_fs_absolute_url() {
    let params = StorageParams::Fs(StorageFsConfig {
        root: "/abs/path/".to_string(),
    });
    assert_eq!(params.url().as_deref(), Some("fs:///abs/path/"));
}

#[test]
fn test_http_glob_url() {
    let params = StorageParams::Http(StorageHttpConfig {
        endpoint_url: "https://example.com/".to_string(),
        paths: vec![
            "/tmp-a.csv".to_string(),
            "/tmp-b.csv".to_string(),
            "/tmp-c.csv".to_string(),
        ],
        network_config: None,
    });
    assert_eq!(
        params.url().as_deref(),
        Some("https://example.com{/tmp-a.csv,/tmp-b.csv,/tmp-c.csv}")
    );
}

#[test]
fn test_http_single_path_url() {
    let params = StorageParams::Http(StorageHttpConfig {
        endpoint_url: "https://example.com".to_string(),
        paths: vec!["/tmp.csv".to_string()],
        network_config: None,
    });
    assert_eq!(params.url().as_deref(), Some("https://example.com/tmp.csv"));
}

#[test]
fn test_hdfs_url() {
    let params = StorageParams::Hdfs(StorageHdfsConfig {
        name_node: "hdfs://namenode:8020".to_string(),
        root: "/data/".to_string(),
        network_config: None,
    });
    assert_eq!(params.url().as_deref(), Some("hdfs://namenode:8020/data/"));
}

#[test]
fn test_webhdfs_url() {
    let params = StorageParams::Webhdfs(StorageWebhdfsConfig {
        endpoint_url: "https://host:50070".to_string(),
        root: "/stage/".to_string(),
        delegation: "".to_string(),
        disable_list_batch: true,
        user_name: "".to_string(),
        network_config: None,
    });
    assert_eq!(params.url().as_deref(), Some("webhdfs://host:50070/stage/"));
}

#[test]
fn test_ftp_url() {
    let params = StorageParams::Ftp(StorageFtpConfig {
        endpoint: "ftps://example.com:21".to_string(),
        root: "/files/".to_string(),
        username: "".to_string(),
        password: "".to_string(),
        network_config: None,
    });
    assert_eq!(
        params.url().as_deref(),
        Some("ftps://example.com:21/files/")
    );
}

#[test]
fn test_ftp_missing_endpoint() {
    let params = StorageParams::Ftp(StorageFtpConfig {
        endpoint: "".to_string(),
        root: "/files/".to_string(),
        username: "".to_string(),
        password: "".to_string(),
        network_config: None,
    });
    assert_eq!(params.url(), None);
}

#[test]
fn test_huggingface_url() {
    let params = StorageParams::Huggingface(StorageHuggingfaceConfig {
        repo_id: "org/dataset".to_string(),
        repo_type: "dataset".to_string(),
        revision: "main".to_string(),
        token: "".to_string(),
        root: "/subset/".to_string(),
        network_config: None,
    });
    assert_eq!(params.url().as_deref(), Some("hf://org/dataset/subset/"));
}

#[test]
fn test_ipfs_url() {
    let params = StorageParams::Ipfs(StorageIpfsConfig {
        endpoint_url: "https://ipfs.example.com".to_string(),
        root: "/ipfs/QmHash/".to_string(),
        network_config: None,
    });
    assert_eq!(params.url().as_deref(), Some("ipfs://ipfs/QmHash/"));
}

#[test]
fn test_memory_url_none() {
    assert_eq!(StorageParams::Memory.url(), None);
}

#[test]
fn test_none_url_none() {
    assert_eq!(StorageParams::None.url(), None);
}

#[test]
fn test_moka_url_none() {
    assert_eq!(
        StorageParams::Moka(StorageMokaConfig::default()).url(),
        None
    );
}

#[test]
fn test_has_credentials_matrix() {
    let s3 = StorageS3Config {
        access_key_id: "ak".to_string(),
        ..Default::default()
    };

    let az = StorageAzblobConfig {
        account_key: "key".to_string(),
        ..Default::default()
    };

    let ftp = StorageFtpConfig {
        username: "user".to_string(),
        ..Default::default()
    };

    let gcs = StorageGcsConfig {
        credential: "cred".to_string(),
        ..Default::default()
    };

    let obs = StorageObsConfig {
        access_key_id: "ak".to_string(),
        ..Default::default()
    };

    let oss = StorageOssConfig {
        access_key_secret: "sk".to_string(),
        ..Default::default()
    };

    let cos = StorageCosConfig {
        secret_id: "id".to_string(),
        ..Default::default()
    };

    let hf = StorageHuggingfaceConfig {
        token: "token".to_string(),
        ..Default::default()
    };

    let webhdfs = StorageWebhdfsConfig {
        delegation: "delegation".to_string(),
        ..Default::default()
    };

    let cases = vec![
        (StorageParams::S3(s3), true, "s3"),
        (StorageParams::Azblob(az), true, "azblob"),
        (StorageParams::Ftp(ftp), true, "ftp"),
        (StorageParams::Gcs(gcs), true, "gcs"),
        (StorageParams::Obs(obs), true, "obs"),
        (StorageParams::Oss(oss), true, "oss"),
        (StorageParams::Cos(cos), true, "cos"),
        (StorageParams::Huggingface(hf), true, "huggingface"),
        (StorageParams::Webhdfs(webhdfs), true, "webhdfs"),
        (StorageParams::Fs(StorageFsConfig::default()), false, "fs"),
        (
            StorageParams::Http(StorageHttpConfig::default()),
            false,
            "http",
        ),
        (
            StorageParams::Ipfs(StorageIpfsConfig::default()),
            false,
            "ipfs",
        ),
        (
            StorageParams::Hdfs(StorageHdfsConfig::default()),
            false,
            "hdfs",
        ),
        (StorageParams::Memory, false, "memory"),
        (
            StorageParams::Moka(StorageMokaConfig::default()),
            false,
            "moka",
        ),
        (StorageParams::None, false, "none"),
    ];

    for (params, expected, label) in cases {
        assert_eq!(params.has_credentials(), expected, "case {}", label);
    }
}

#[test]
fn test_has_encryption_key() {
    let mut s3 = StorageS3Config::default();
    assert!(!StorageParams::S3(s3.clone()).has_encryption_key());
    s3.master_key = "mk".to_string();
    assert!(StorageParams::S3(s3).has_encryption_key());

    let mut oss = StorageOssConfig::default();
    assert!(!StorageParams::Oss(oss.clone()).has_encryption_key());
    oss.server_side_encryption = "KMS".to_string();
    assert!(StorageParams::Oss(oss).has_encryption_key());
}

#[test]
fn test_endpoint_for_storages_with_endpoint() {
    let s3 = StorageS3Config {
        endpoint_url: "https://s3.example.com".to_string(),
        ..Default::default()
    };
    assert_eq!(
        StorageParams::S3(s3).endpoint().as_deref(),
        Some("https://s3.example.com")
    );

    let http = StorageParams::Http(StorageHttpConfig {
        endpoint_url: "https://files.example.com".to_string(),
        paths: vec![],
        network_config: None,
    });
    assert_eq!(
        http.endpoint().as_deref(),
        Some("https://files.example.com")
    );
}

#[test]
fn test_redacted_for_display_masks_secrets() {
    let original = StorageParams::S3(StorageS3Config {
        access_key_id: "ACCESS123".to_string(),
        secret_access_key: "SECRET456".to_string(),
        security_token: "TOKEN789".to_string(),
        master_key: "MASTER000".to_string(),
        ..Default::default()
    });

    if let StorageParams::S3(redacted) = original.redacted_for_display() {
        assert_eq!(
            redacted.access_key_id,
            mask_string("ACCESS123", 3),
            "access key should be masked"
        );
        assert_eq!(
            redacted.secret_access_key,
            mask_string("SECRET456", 3),
            "secret key should be masked"
        );
        assert_eq!(
            redacted.security_token,
            mask_string("TOKEN789", 3),
            "token should be masked"
        );
        assert_eq!(
            redacted.master_key,
            mask_string("MASTER000", 3),
            "master key should be masked"
        );
    } else {
        unreachable!("redacted clone must remain S3 variant");
    }
}

#[test]
fn test_endpoint_none_when_missing() {
    assert_eq!(
        StorageParams::Fs(StorageFsConfig::default()).endpoint(),
        None
    );

    let ftp = StorageParams::Ftp(StorageFtpConfig {
        endpoint: "".to_string(),
        ..Default::default()
    });
    assert_eq!(ftp.endpoint(), None);
}
