import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting Started

Let's start your Databend journey!

There's a lot to learn, but every journey starts somewhere. In this section, we will learn how to get started with Databend.

## Setup

First, we need to get our development environment ready. Please pick your favorite style to get started with:

<Tabs>
<TabItem value="Use setup script (quick and easy)">

```shell
git clone git@github.com:datafuselabs/databend.git
cd databend
make setup
```

</TabItem>
<TabItem value="Manually (advanced)">

- Step 1: Install Rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

- Step 2: Install tools that we need

  - python3
  - docker
  - mysql

</TabItem>
<TabItem value="Docker">

```shell
docker pull datafuselabs/dev-container
```
</TabItem>
</Tabs>

## Build

```shell
make build
```

## Test

```shell
make test
```

Please refer to the [Contributing](contributing) to know more information about how to contribute to Databend.
