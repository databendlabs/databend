---
title: Downloading Databend
sidebar_label: Downloading Databend
description:
  Downloading Databend
---

Databend offers you these options for downloading the installation packages:

- [Manual download](#manual-download): You can download the installation package for your platform directly from the Databend website.
- [APT Package Manager](#apt-package-manager): You can use the APT package manager to download and install Databend on Ubuntu or Debian.
- [Docker](#docker): You can use Docker to download and run Databend in a containerized environment.

## Manual Download

The primary distribution packages for Databend are `.tar.gz` archives containing single executable files that you can download from the [Download](https://databend.rs/download) page and extract them anywhere on your system. 

## APT Package Manager

Databend offers package repositories for Debian and Ubuntu systems, allowing you to install Databend using the apt install command or any other APT frontend.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="distributions">
<TabItem value="deb-old" label="Ubuntu/Debian">

```shell
sudo curl -L -o /usr/share/keyrings/datafuselabs-keyring.gpg https://repo.databend.rs/deb/datafuselabs.gpg
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.list https://repo.databend.rs/deb/datafuselabs.list

sudo apt update

sudo apt install databend
```
</TabItem>

<TabItem value="deb-new" label="Ubuntu/Debian(DEB822-STYLE FORMAT)">

:::note
Available platforms:
  * Ubuntu Jammy (22.04) and later
  * Debian Bookworm(12) and later
:::

```shell
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.sources https://repo.databend.rs/deb/datafuselabs.sources

sudo apt update

sudo apt install databend
```
</TabItem>
</Tabs>

:::tip
To start Databend after the installation, run the following commands:

```shell
sudo systemctl start databend-meta
sudo systemctl start databend-query
```
:::

## Docker

Databend provides these types of installation images on the Docker Hub:

- [Databend All-in-One Docker Image](https://hub.docker.com/r/datafuselabs/databend): Built for local tests, CI, and so on.
- Separated images: Built for production environments, Kubernetes, and [Helm Charts](https://github.com/datafuselabs/helm-charts).
  - [databend-meta](https://hub.docker.com/r/datafuselabs/databend-meta)
  - [databend-query](https://hub.docker.com/r/datafuselabs/databend-query)

Click the links above for their detailed instructions.