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

# Using Podman instead of Docker

Iceberg-rust does not require containerization, except for integration tests, where "docker" and "docker-compose" are used to start containers for minio and various catalogs. Below instructions setup "rootful podman" and docker's official docker-compose plugin to run integration tests as an alternative to docker or Orbstack.

1. Have podman v4 or newer.
    ```shell
    $ podman --version
    podman version 4.9.4-rhel
    ```
2. Create a docker wrapper script:
   
  * Create a fresh `/usr/bin/docker` file and add the below contents:
    ```bash
    #!/bin/sh
    [ -e /etc/containers/nodocker ] || \
    echo "Emulate Docker CLI using podman. Create /etc/containers/nodocker to quiet msg." >&2
    exec sudo /usr/bin/podman "$@"
    ```

  * Set new `/usr/bin/docker` file to executable.
    ```shell
    sudo chmod +x /usr/bin/docker
    ```

3. Install the [docker compose plugin](https://docs.docker.com/compose/install/linux). Check for successful installation.
    ```shell
    $ docker compose version
    Docker Compose version v2.28.1
    ```

4. Append the below to `~/.bashrc` or equivalent shell config:
    ```bash
    export DOCKER_HOST=unix:///run/podman/podman.sock
    ```

5. Start the "rootful" podman socket.
    ```shell
    sudo systemctl start podman.socket
    sudo systemctl status podman.socket
    ```

6. Check that the following symlink exists.
    ```shell
    $ ls -al /var/run/docker.sock
    lrwxrwxrwx 1 root root 27 Jul 24 12:18 /var/run/docker.sock -> /var/run/podman/podman.sock
    ```
    If the symlink does not exist, create it.
    ```shell
    sudo ln -s /var/run/podman/podman.sock /var/run/docker.sock
    ```

7. Check that the docker socket is working.
    ```shell
    sudo curl -H "Content-Type: application/json" --unix-socket /var/run/docker.sock http://localhost/_ping
    ```

8. Try some integration tests!
    ```shell
    cargo test -p iceberg --test file_io_s3_test
    ```

# References

* <https://docs.docker.com/compose/install/linux>
* <https://www.redhat.com/sysadmin/podman-docker-compose>

# Note on rootless containers

As of podman v4, ["To be succinct and simple, when running rootless containers, the container itself does not have an IP address"](https://www.redhat.com/sysadmin/container-ip-address-podman) This causes issues with iceberg-rust's integration tests, which rely upon ip-addressable containers via docker-compose. As a result, podman "rootful" containers are required throughout to ensure containers have IP addresses. Perhaps as a future work or with updates to default podman networking, the need for "rootful" podman containers can be eliminated.

* <https://www.redhat.com/sysadmin/container-ip-address-podman>
* <https://github.com/containers/podman/blob/main/docs/tutorials/basic_networking.md>

# Debugging Note:
 - Fix for error: `Error: short-name "apache/iceberg-rest-fixture" did not resolve to an alias and no unqualified-search registries are defined in "/etc/containers/registries.conf"`
    - Add or modify the `/etc/containers/registries.conf` file:
    ```toml
    [[registry]]
    prefix = "docker.io"
    location = "docker.io"
    ```
