<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg™ Rust Downloads

The official Apache Iceberg-Rust releases are provided as source artifacts.

## Releases

The latest source release is [0.7.0](https://www.apache.org/dyn/closer.lua/iceberg/apache-iceberg-rust-0.7.0/apache-iceberg-rust-0.7.0-src.tar.gz) ([asc](https://downloads.apache.org/iceberg/apache-iceberg-rust-0.7.0/apache-iceberg-rust-0.7.0-src.tar.gz.asc),
[sha512](https://downloads.apache.org/iceberg/apache-iceberg-rust-0.7.0/apache-iceberg-rust-0.7.0-src.tar.gz.sha512)).

For older releases, please check the [archive](https://archive.apache.org/dist/iceberg/).

## Notes

* When downloading a release, please verify the OpenPGP compatible signature (or failing that, check the SHA-512); these should be fetched from the main Apache site.
* The KEYS file contains the public keys used for signing release. It is recommended that (when possible) a web of trust is used to confirm the identity of these keys.
* Please download the [KEYS](https://downloads.apache.org/iceberg/KEYS) as well as the .asc signature files.

### To verify the signature of the release artifact

You will need to download both the release artifact and the .asc signature file for that artifact. Then verify the signature by:

* Download the KEYS file and the .asc signature files for the relevant release artifacts.
* Import the KEYS file to your GPG keyring:

    ```shell
    gpg --import KEYS
    ```

* Verify the signature of the release artifact using the following command:

    ```shell
    gpg --verify <artifact>.asc <artifact>
    ```

### To verify the checksum of the release artifact

You will need to download both the release artifact and the .sha512 checksum file for that artifact. Then verify the checksum by:

```shell
shasum -a 512 -c <artifact>.sha512
```
