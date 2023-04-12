---
title: Jupyter Notebook
sidebar_label: Jupyter Notebook
description:
  Integrating Databend with Jupyter Notebook.
---

[Jupyter Notebook](https://jupyter.org) is a web-based interactive application that enables you to create notebook documents that feature live code, interactive plots, widgets, equations, images, etc., and share these documents easily. It is also quite versatile as it can support many programming languages via kernels such as Julia, Python, Ruby, Scala, Haskell, and R.

With the SQLAlchemy library in Python or [ipython-sql](https://github.com/catherinedevlin/ipython-sql), you can establish a connection to Databend within a Jupyter Notebook, allowing you to execute queries and visualize your data from Databend directly in the Notebook.

## Tutorial: Integrate with Jupyter Notebook using SQLAlchemy

In this tutorial, you will first deploy a local Databend instance and Jupyter Notebook, and then run a sample notebook to connect to your local Databend through the SQLAlchemy library, as well as write and visualize data within the notebook.

Before you start, make sure you have completed the following tasks:

- You have [Python](https://www.python.org/) installed on your system.
- Download the sample notebook [databend.ipynb](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/databend.ipynb) to a local folder.

### Step 1. Deploy Databend

1. Follow the [Deployment Guide](https://databend.rs/doc/deploy) to deploy a local Databend.
2. Create a SQL user in Databend. You will use this account to connect to Databend in Jupyter Notebook.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL ON *.* TO user1;
```

### Step 2. Deploy Jupyter Notebook

1. Install Jupyter Notebook with pip:

```shell
pip install notebook
```

2. Install dependencies with pip:

```shell
pip install sqlalchemy
pip install pandas
pip install pymysql
```

### Step 3. Run Sample Notebook

1. Run the command below to start Jupyter Notebook:

```shell
jupyter notebook
```

  This will start up Jupyter and your default browser should start (or open a new tab) to the following URL: http://localhost:8888/tree

![Alt text](../../../public/img/integration/notebook-tree.png)

2. On the **Files** tab, navigate to the sample notebook you downloaded and open it.

3. In the sample notebook, run the cells sequentially. By doing so, you create a table containing 5 rows in your local Databend, and visualize the data with a bar chart.

![Alt text](../../../public/img/integration/integration-gui-jupyter.png)

## Tutorial: Integrate with Jupyter Notebook using ipython-sql

In this tutorial, you will first deploy a local Databend instance and Jupyter Notebook, and then run a sample notebook to connect to your local Databend through [ipython-sql](https://github.com/catherinedevlin/ipython-sql), as well as write and visualize data within the notebook.

Before you start, ensure that you have [Python](https://www.python.org/) installed on your system.

### Step 1. Deploy Databend

1. Follow the [Deployment Guide](https://databend.rs/doc/deploy) to deploy a local Databend.
2. Create a SQL user in Databend. You will use this account to connect to Databend in Jupyter Notebook.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL ON *.* TO user1;
```

### Step 2. Deploy Jupyter Notebook

1. Install Jupyter Notebook with pip:

```shell
pip install notebook
```

2. Install dependencies with pip:

```shell
pip install ipython-sql databend-sqlalchemy
pip install sqlalchemy
```

### Step 3. Create and Connect a Notebook to Databend

1. Run the command below to start Jupyter Notebook:

```shell
jupyter notebook
```

  This will start up Jupyter and your default browser should start (or open a new tab) to the following URL: http://localhost:8888/tree

![Alt text](../../../public/img/integration/notebook-tree.png)

2. Select **New** > **Python 3** to create a notebook.

3. Run the following code sequentially in separate cells. By doing so, you create a table containing 5 rows in your local Databend, and visualize the data with a bar chart.

```python
%load_ext sql
```

```sql
%%sql databend://user1:abc123@localhost:8000/default
create table if not exists user(created_at Date, count Int32);
insert into user values('2022-04-01', 5);
insert into user values('2022-04-01', 3);
insert into user values('2022-04-03', 4);
insert into user values('2022-04-03', 1);
insert into user values('2022-04-04', 10);
```

```python
result = %sql select created_at as date, count(*) as count from user group by created_at;
result
```

```python
%matplotlib inline

df = result.DataFrame()
df.plot.bar(x='date', y='count')
```
You can now see a bar chart on the notebook:

![Alt text](../../../public/img/integration/jupyter-ipython-sql.png)