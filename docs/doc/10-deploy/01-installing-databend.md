---
title: Installing Databend
sidebar_label: Installing Databend
description:
  Installing Databend
---

## Package Manager

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="distributions">

<TabItem value="deb-old" label="Ubuntu/Debian(old)">

```shell

sudo curl -L -o /usr/share/keyrings/datafuselabs-keyring.gpg https://repo.databend.rs/deb/datafuselabs.gpg
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.list https://repo.databend.rs/deb/datafuselabs.list

sudo apt update

sudo apt install databend
```

</TabItem>

<TabItem value="deb-new" label="Ubuntu/Debian(new)">

:::caution
DEB822 Source Format

Supported by:
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
To run databend:

* `sudo systemctl start databend-meta`
* `sudo systemctl start databend-query`
:::


## Docker

### All-in-one docker image

https://hub.docker.com/r/datafuselabs/databend

:::note
Designed for local testing, CI, etc.

Refer to DockerHub Readme for detailed usage.
:::


### Separated service image

https://hub.docker.com/r/datafuselabs/databend-meta

https://hub.docker.com/r/datafuselabs/databend-query

:::note
Designed for production, mostly used by Kubernetes, [Helm Chart](https://github.com/datafuselabs/helm-charts).

Refer to [Deploying Databend on Kubernetes](./04-deploying-databend-on-kubernetes.md) for detailed usage.
:::


## Manually Download

1. Go to [Download Page](https://databend.rs/download) and download the latest package for your platform.

2. Extract the installation package to a local directory.
