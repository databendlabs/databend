// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::net::IpAddr;
use std::process::Command;

use tracing::error;

use crate::cmd::{get_cmd_output, get_cmd_output_result, run_command};

/// A utility to manage the lifecycle of `docker compose`.
///
/// It will start `docker compose` when calling the `run` method and will be stopped via [`Drop`].
#[derive(Debug)]
pub struct DockerCompose {
    project_name: String,
    docker_compose_dir: String,
}

impl DockerCompose {
    pub fn new(project_name: impl ToString, docker_compose_dir: impl ToString) -> Self {
        Self {
            project_name: project_name.to_string(),
            docker_compose_dir: docker_compose_dir.to_string(),
        }
    }

    pub fn project_name(&self) -> &str {
        self.project_name.as_str()
    }

    fn get_os_arch() -> String {
        let mut cmd = Command::new("docker");
        cmd.arg("info")
            .arg("--format")
            .arg("{{.OSType}}/{{.Architecture}}");

        let result = get_cmd_output_result(cmd, "Get os arch".to_string());
        match result {
            Ok(value) => value.trim().to_string(),
            Err(_err) => {
                // docker/podman do not consistently place OSArch info in the same json path across OS and versions
                // Below tries an alternative path if the above path fails
                let mut alt_cmd = Command::new("docker");
                alt_cmd
                    .arg("info")
                    .arg("--format")
                    .arg("{{.Version.OsArch}}");
                get_cmd_output(alt_cmd, "Get os arch".to_string())
                    .trim()
                    .to_string()
            }
        }
    }

    pub fn up(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.env("DOCKER_DEFAULT_PLATFORM", Self::get_os_arch());

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "up",
            "-d",
            "--wait",
            "--timeout",
            "1200000",
        ]);

        let ret = run_command(
            cmd,
            format!(
                "Starting docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        );

        if !ret {
            let mut cmd = Command::new("docker");
            cmd.current_dir(&self.docker_compose_dir);

            cmd.env("DOCKER_DEFAULT_PLATFORM", Self::get_os_arch());

            cmd.args(vec![
                "compose",
                "-p",
                self.project_name.as_str(),
                "logs",
                "spark-iceberg",
            ]);
            run_command(cmd, "Docker compose logs");
            panic!("Docker compose up failed!")
        }
    }

    pub fn down(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "down",
            "-v",
            "--remove-orphans",
        ]);

        let ret = run_command(
            cmd,
            format!(
                "Stopping docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        );

        if !ret {
            panic!("Failed to stop docker compose")
        }
    }

    pub fn get_container_ip(&self, service_name: impl AsRef<str>) -> IpAddr {
        let container_name = format!("{}-{}-1", self.project_name, service_name.as_ref());
        let mut cmd = Command::new("docker");
        cmd.arg("inspect")
            .arg("-f")
            .arg("{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}")
            .arg(&container_name);

        let ip_result = get_cmd_output(cmd, format!("Get container ip of {container_name}"))
            .trim()
            .parse::<IpAddr>();
        match ip_result {
            Ok(ip) => ip,
            Err(e) => {
                error!("Invalid IP, {e}");
                panic!("Failed to parse IP for {container_name}")
            }
        }
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        self.down()
    }
}
