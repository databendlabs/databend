---
title: Visualization Databend data in Jupyter Notebook
sidebar_label: Jupyter
description:
  Visualization Databend data in Jupyter Notebook.
---


<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-jupyter-databend.png" width="550"/>
</p>

## What is [Jupyter Notebook](https://jupyter.org/)?

The Jupyter Notebook is the original web application for creating and sharing computational documents. It offers a simple, streamlined, document-centric experience.

## Jupyter

### Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```shell title='mysql>'
create user user1 identified by 'abc123';
```

Grant privileges for the user:
```shell title='mysql>'
grant all on *.* to user1;
```

See also [How To Create User](../../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md).

### Install Jupyter Python Package

Jupyter:
```shell
pip install jupyterlab
pip install notebook
```

Dependencies:
```shell
pip install sqlalchemy
pip install pandas
```

### Run Jupyter Notebook

Download [**databend.ipynb**](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/databend.ipynb), start the notebook:
```shell
jupyter notebook
```

Run `databend.ipynb` step by step, the demo show:
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-gui-jupyter.png"/>
</p>
