# Develop Environment Setup

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Build Essential Setup

Install `git`, `make` and so on.

<Tabs>
<TabItem value="apt">

```shell
apt install build-essential
```
</TabItem>

<TabItem value="yum/dnf">

```shell
yum groupinstall 'Development Tools'
```
</TabItem>

<TabItem value="pacman">

```shell
pacman -S base-devel
```
</TabItem>
</Tabs>

## Get the code

```shell
git clone git@github.com:datafuselabs/databend.git
```

## Setup develop environment

### Use setup script (quick and easy)

```shell
make setup
```

### Manually (advanced)

#### Step 1: Install rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Step 2: Install tools that we need

TBD

## Build the project

```shell
cargo build
```

---

If you encounter any problems during your journey, do not hesitate to reach out on the [discussions](https://github.com/datafuselabs/databend/discussions/categories/q-a).
