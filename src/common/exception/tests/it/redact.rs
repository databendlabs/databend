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

use std::borrow::Cow;

use databend_common_exception::ErrorCode;
use databend_common_exception::SerializedError;
use databend_common_exception::redact_url_secrets;

// A JWT shaped web identity token.
const TOKEN: &str = concat!(
    "eyJhbGciOiJSUzI1NiJ9",
    ".",
    "eyJzdWIiOiJub2RlIn0",
    ".",
    "c2ln_-"
);
// IAM role names may contain `,`, `+`, `=`, `.`, `@`.
const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/databend,query+x=y.z@w";

fn sts_url() -> String {
    format!(
        "https://sts.zz-nowhere-1.amazonaws.com/?Action=AssumeRoleWithWebIdentity&RoleArn={ROLE_ARN}&WebIdentityToken={TOKEN}&Version=2011-06-15&RoleSessionName=s"
    )
}

fn assert_redacted(message: &str) {
    assert!(!message.contains(TOKEN), "{message}");
    assert!(!message.contains(ROLE_ARN), "{message}");
    assert!(
        message.contains("RoleArn=***&WebIdentityToken=***&"),
        "{message}"
    );
}

fn assert_no_secret(text: &str) {
    assert!(!text.contains(TOKEN), "{text}");
    assert!(!text.contains(ROLE_ARN), "{text}");
}

#[test]
fn test_redact_url_secrets() {
    // reqwest quotes the URL in parentheses when the request can't be sent.
    let sts = format!("error sending request for url ({})", sts_url());
    assert_eq!(
        redact_url_secrets(&sts),
        "error sending request for url (https://sts.zz-nowhere-1.amazonaws.com/?Action=AssumeRoleWithWebIdentity&RoleArn=***&WebIdentityToken=***&Version=2011-06-15&RoleSessionName=s)"
    );

    let cases = [
        (
            "uri: http://nn:9870/webhdfs/v1/a?op=OPEN&delegation=HAAEaGRmcwRoZGZz",
            "uri: http://nn:9870/webhdfs/v1/a?op=OPEN&delegation=***",
        ),
        (
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=AKIA%2F1&X-Amz-Security-Token=IQo%2Bb3==&x-amz-signature=abc",
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=***&X-Amz-Security-Token=***&x-amz-signature=***",
        ),
        (
            "https://a.blob.core.windows.net/c/k?sv=2022-11-02&sig=AbC%2Bd%3D \"next\"",
            "https://a.blob.core.windows.net/c/k?sv=2022-11-02&sig=*** \"next\"",
        ),
        (
            "url: \"WebIdentityToken=abc&x=1\"",
            "url: \"WebIdentityToken=***&x=1\"",
        ),
        (
            "https://storage.googleapis.com/b/o?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=sa%40p.iam.gserviceaccount.com%2F20240101%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20240101T000000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=0a1b2c",
            "https://storage.googleapis.com/b/o?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=***&X-Goog-Date=20240101T000000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=***",
        ),
        // Values keep going through characters a URL may carry unescaped.
        ("?token=user:pass@host/x#frag", "?token=***#frag"),
        (
            "?access_token=prefix,suffix&next=1",
            "?access_token=***&next=1",
        ),
        ("?token=a'b(c)d&x=1", "?token=***&x=1"),
        (
            "error sending request for url (https://h/?token=a(b)c)",
            "error sending request for url (https://h/?token=***)",
        ),
        // Trailing quotes, parentheses and commas close the text around a URL.
        (
            "url: 'https://h/?token=abc', then",
            "url: 'https://h/?token=***', then",
        ),
        ("(https://h/?sig=abc),", "(https://h/?sig=***),"),
        // An ambiguous boundary masks more, never less.
        ("('https://h/?token=abc','x')", "('https://h/?token=***')"),
        (
            "{\"url\":\"https://h/?sig=abc\"}",
            "{\"url\":\"https://h/?sig=***\"}",
        ),
        ("?token=abc", "?token=***"),
    ];
    for (input, expected) in cases {
        assert_eq!(redact_url_secrets(input), expected, "{input}");
    }

    // Unrelated messages and parameters are untouched and not copied.
    for input in [
        "",
        "path not found: a/b?c",
        "https://host/k?versionId=1&partNumber=2",
        "trailing separator &",
        "tokens=1&signatures=2",
        "?token=&x=1",
        "SELECT token=1, sig=2 FROM t",
    ] {
        assert!(
            matches!(redact_url_secrets(input), Cow::Borrowed(_)),
            "{input}"
        );
    }
}

#[test]
fn test_error_code_masks_url_credentials() {
    let url = sts_url();
    let opendal_error = || {
        opendal::Error::new(opendal::ErrorKind::Unexpected, "request failed")
            .with_context("uri", &url)
            .set_source(anyhow::anyhow!("error sending request for url ({url})"))
    };

    // Conversions.
    let errors = vec![
        ErrorCode::from(opendal_error()),
        ErrorCode::from(
            opendal::Error::new(opendal::ErrorKind::NotFound, "missing").with_context("uri", &url),
        ),
        ErrorCode::from(std::io::Error::other(format!("io ({url})"))),
        ErrorCode::from(anyhow::anyhow!("anyhow ({url})")),
        // Call sites that format a storage error themselves.
        ErrorCode::StorageUnavailable(format!("storage is not valid, cause: {}", opendal_error())),
        ErrorCode::StorageOther(opendal_error().to_string()),
        ErrorCode::StorageOther("permission check failed")
            .add_message(format!("[Write] failed: {}", opendal_error())),
        ErrorCode::StorageOther("permission check failed")
            .add_detail(format!("[Read] failed: {}", opendal_error())),
        ErrorCode::StorageOther("x").add_message_back(url.clone()),
        ErrorCode::StorageOther("x").add_detail_back(url.clone()),
    ];

    for error in errors {
        assert_redacted(&error.message());
        // The secret may sit in the detail part only.
        assert_no_secret(&error.display_text());
        assert_redacted(&format!("{error}"));
        assert_redacted(&format!("{error:?}"));
        assert_redacted(&error.clone().message());
        // Flight / tonic transport between nodes.
        assert_redacted(&SerializedError::from(&error).message);
        assert_redacted(&ErrorCode::from(&SerializedError::from(&error)).message());
        assert_redacted(tonic::Status::from(error).message());
    }

    let with_detail = ErrorCode::StorageOther("x").add_detail(url.clone());
    assert_redacted(&with_detail.detail());
}
