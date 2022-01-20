# Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

## Get Started

Specify node version

```shell
nvm use
```

Install dependencies

```shell
yarn install
```

Local preview

```shell
yarn start
```

Production build

```shell
yarn build
```

## Features

### Tabs

```markdown
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="binary" label="Release binary" default>
    ...
</TabItem>
<TabItem value="docker" label="Run with Docker(Recommended)">
    ...
</TabItem>
</Tabs>
```

Take [Deploy](../docs/user/index.md) for example. Visit [Docusaurus - Tabs](https://docusaurus.io/docs/markdown-features/tabs) for detailed reference.

### Admonitions

```markdown
:::note

Some **content** with _markdown_ `syntax`.

:::
```

Take [Deploy](../docs/user/index.md) for example. Visit [Docusaurus - Admonitions](https://docusaurus.io/docs/markdown-features/admonitions) for detailed reference.
