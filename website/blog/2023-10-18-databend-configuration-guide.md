---
title: "Navigating Databend's Configuration Maze: A Guide for Developers and Operators"
date: 2023-10-18
slug: 2023-10-18-databend-configuration-guide
cover_url: 'configuration-guide.png'
image: 'configuration-guide.png'
tags: ["configuration", "DBA", "source reading"]
description: "This blog post explores the configuration management in Databend. It discusses the three supported configuration options: command-line arguments, environment variables, and configuration files, with their respective priorities. The post also delves into the mapping of configurations in Databend Query and Databend Meta, providing code examples and explanations."
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

# Navigating Databend's Configuration Maze: A Guide for Developers and Operators

[Databend](https://github.com/datafuselabs/databend), a powerful data warehouse,
offers a myriad of configuration options for its Query and Meta services.
Understanding and managing these configurations can be overwhelming, especially
when it comes to the various formats and syntaxes used in different
environments. In this post, we'll demystify Databend's configuration landscape
and provide a comprehensive guide to help developers and operators alike
successfully navigate its complexities.

## Configuration Basics

Before diving into the specifics of Query and Meta configurations, it's
essential to understand the basics. Databend supports three configuration
methods, each with its own priority and use case:

1. Command-line options: Useful for temporary, local overrides of environment
   variables or configuration files.
2. Environment variables: Ideal for Kubernetes and other cloud environments,
   allowing for flexible configuration changes without altering the
   configuration files.
3. Configuration files: The recommended approach for most use cases, providing a
   structured and version-controlled way to manage configurations.

## Configuration in Databend Query

Regardless of the configuration method used, the configuration options in
Databend Query can be seen as a flattened tree-like mapping of code, following
the logic of "configuration domain" + "configuration item."

- In environment variables and configuration files, the code is flattened using
  [serfig](https://crates.io/crates/serfig), with `_` used as a separator.
- Command-line options differ slightly: they use `-` as a separator, and some
  command-line arguments do not have a bound configuration domain.

### Example: `admin_api_address` Configuration

To better understand the mapping relationship, let's start with the
`admin_api_address` configuration option. This setting determines the address
and port that the admin API will listen on.

- In a TOML configuration file, the setting is defined as follow. The `query`
  section represents the configuration domain, and the `admin_api_address` is
  the specific option within that domain.

  ```toml
  [query]
  ...
  # Databend Query http address. For admin RESET API.
  admin_api_address = "0.0.0.0:8080"
  ...
  ```

- In environment variable, it is represented as `QUERY_ADMIN_API_ADDRESS`, where
  `QUERY` represents the configuration domain and `ADMIN_API_ADDRESS` is the
  specific configuration option. To set the admin API address using an
  environment variable, you can use the following command:

  ```
  export QUERY_ADMIN_API_ADDRESS=0.0.0.0:8080
  ```

- Command-line option, however, do not follow the same naming convention as
  environment variable or configuration file. Instead, the configuration option
  is used. So, you can adjust this value using the `--admin-api-address` command
  line argument. For example, to set the admin API address to `0.0.0.0:8081`,
  you can use the following command:

  ```
  databend-query --admin-api-address=0.0.0.0:8080
  ```

> **Note**: In this case, command-line option is not bound to a specific
> configuration domain. However, if configuring `--storage-s3-access-key-id`,
> the configuration domain would be "storage" + "s3", and "access-key-id" would
> be the specific configuration option. You can use "databend-query --help" to
> view all supported command-line arguments.

## Mapping Configuration Options to Code

Let's dive into the code related to configuration to further understand the
mapping relationship (located in `src/query/config/src/config.rs`):

```rust
pub struct Config {
    ...
    #[clap(flatten)]
    pub query: QueryConfig,
    ...
}

/// Query config group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    ...
    #[clap(long, default_value = "127.0.0.1:8080")]
    pub admin_api_address: String,
    ...
}
```

In the code, the top-level structure is `Config`, and `admin_api_address` is a
configuration option within `pub query: QueryConfig`. After processing with
`serfig`, it needs to be represented by either "QUERY" or "[query]" to indicate
its configuration domain, while the configuration option remains as
`admin_api_address`.

For command-line options, the specific option name and default value are
controlled by `#[clap(long = "<long-name>", default_value = "<value>")]`.
[clap](https://crates.io/crates/clap) takes over the configuration:

- `admin_api_address` becomes `--admin-api-address`.
- For `--storage-s3-access-key-id`, the actual code hierarchy is
  `Config -> StorageConfig -> S3StorageConfig -> access_key_id`, annotated with
  `#[clap(long = "storage-s3-access-key-id", default_value_t)]`. Therefore, it
  needs to be configured with `--storage-s3-access-key-id`.

## Configuration in Databend Meta

The Meta service's configuration follows a similar structure to the Query
service, with command-line arguments and configuration files playing a similar
form. However, environment variables are managed using custom one-to-one
mappings (powered by `serde-env` in serfig), but can still be understood based
on "configuration domain" + "configuration option".

### Example: `log_dir` Configuration

Let's explore the mapping between the different configuration methods for the
`log_dir` option:

- In the configuration file, it applies globally and can be set as:

  ```toml
  log_dir = "./.databend/logs"
  ```

- In environment variables, it needs to be set as `METASRV_LOG_DIR`, where
  `METASRV` represents the configuration domain, and "LOG_DIR" is the specific
  configuration option.

  ```
  expert METASRV_LOG_DIR=./.databend/logs
  ```

- For command-line configuration, it can be set directly using `--log-dir` .

  ```bash
  databend-meta --log-dir=./.databend/logs
  ```

### Mapping Configuration Options to Code

Now, let's deconstruct the mapping through the code (located in
`src/meta/service/src/configs/outer_v0.rs`):

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
#[serde(default)]
pub struct Config {
    ...
    /// Log file dir
    #[clap(long = "log-dir", default_value = "./.databend/logs")]
    pub log_dir: String,
    ...
}
```

The configuration options related to configuration files and command-line
options are managed by the `Config` structure, following the same logic as
Databend Query.

For environment variables, the configuration options are processed by the
`ConfigViaEnv` structure, which is generated by the macro `serde::Deserialize`,
based on the `Config` structure.

```rust
/// #[serde(flatten)] doesn't work correctly for env.
/// We should work around it by flatten them manually.
/// We are seeking for better solutions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConfigViaEnv {
    ...
    pub metasrv_log_dir: String,
    ...
}

// Implement Into target on ConfigViaEnv to make the transform logic more clear.
#[allow(clippy::from_over_into)]
impl Into<Config> for ConfigViaEnv {
    fn into(self) -> Config {
        ...
        Config {
            // cmd, key, value and prefix should only be passed in from CLI
            ...
            log_dir: self.metasrv_log_dir,
            ...
        }
    }
}
```

Therefore, `metasrv_log_dir` corresponds to the environment variable
`METASRV_LOG_DIR`, and ultimately maps to the `log_dir` option in `Config`, thus
associating it with the configuration file and command line option `--log-dir`.

## Implicit Environment Variables

While some environment variables is not a formal option listed in the
configuration options, it can still be used as an environment variable for
configuration purposes.

This is mainly due to the absence of setting corresponding configuration
options, which triggers the rollback mechanism of
[reqsign](https://crates.io/crates/reqsign). It reads some commonly used
environment variables of the corresponding service to ensure its proper
operation as much as possible.

The mapping and usage of these environment variables can be understood by
examining the relevant code sections.

### Example: `AWS_ACCESS_KEY_ID` Environment Variable

In the implementation, there is a check for the `STORAGE_S3_ACCESS_KEY_ID`
configuration option, which aligns with the configuration and environment
variable mapping.

However, if the corresponding configuration is not provided, a fallback
mechanism is triggered. `reqsign` will check `AWS_S3_ACCESS_KEY_ID` to obtain
possible value.

This is also the reason why commands like the one below can work correctly.

```bash
docker run \
    -p 8000:8000 \
    -p 3307:3307 \
    -v meta_storage_dir:/var/lib/databend/meta \
    -v query_storage_dir:/var/lib/databend/query \
    -v log_dir:/var/log/databend \
    -e QUERY_DEFAULT_USER=databend \
    -e QUERY_DEFAULT_PASSWORD=databend \
    -e QUERY_STORAGE_TYPE=s3 \
    -e AWS_S3_ENDPOINT=http://172.17.0.2:9000 \
    -e AWS_S3_BUCKET=databend \
    -e AWS_ACCESS_KEY_ID=ROOTUSER \
    -e AWS_SECRET_ACCESS_KEY=CHANGEME123 \
    datafuselabs/databend
```

### Mapping Implicit Environment Variables to Code

Let's examine the relevant code snippet in `src/common/storage/src/operator.rs`
(line 244):

```rust
builder.access_key_id(&cfg.access_key_id);
```

The `access_key_id()` function in the default behavior of the opendal library
handles the configuration:

```rust
/// Set access_key_id of this backend.
///
/// - If access_key_id is set, we will take user's input first.
/// - If not, we will try to load it from environment.
pub fn access_key_id(&mut self, v: &str) -> &mut Self {...}
```

Since `disable_config_load` is not explicitly set, the S3 accessor will attempt
to load the configuration from the environment variables while building. The
code snippet responsible for this behavior is found in `AwsConfig` in the
`reqsign` library:

```rust
pub struct AwsConfig {
    /// `access_key_id` will be loaded from:
    /// - this field if it's `is_some`
    /// - env value: `AWS_ACCESS_KEY_ID`
    /// - profile config: `aws_access_key_id`
    pub access_key_id: Option<String>,
    ...
}
```

So if `access_key_id` is not provided, the library will attempt to load it from
the `AWS_ACCESS_KEY_ID` environment variable.

This also allows you to choose familiar AWS or other service examples' default
environment variables when using Databend in practice.

## Conclusion

Understanding configuration management in Databend is crucial for developers and
operators to effectively manage and fine-tune the database server program. By
leveraging command-line arguments, environment variables, and configuration
files, users can tailor the configurations to meet their specific needs. The
code examples and explanations provided in this blog post serve as a guide to
navigating the configuration management in Databend Query and Databend Meta.
