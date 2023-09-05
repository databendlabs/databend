---
title: Jupyter Notebook
---

[Jupyter Notebook](https://jupyter.org) is a web-based interactive application that enables you to create notebook documents that feature live code, interactive plots, widgets, equations, images, etc., and share these documents easily. It is also quite versatile as it can support many programming languages via kernels such as Julia, Python, Ruby, Scala, Haskell, and R.

With the SQLAlchemy library in Python or [ipython-sql](https://github.com/catherinedevlin/ipython-sql), you can establish a connection to Databend within a Jupyter Notebook, allowing you to execute queries and visualize your data from Databend directly in the notebook. Alternatively, you can run SQL queries in Python using the [Databend Python Binding](https://pypi.org/project/databend/) library, allowing you to harness DataBend's capabilities directly within your local Python environment or online services like Jupyter Notebook and Google Colab without the need to deploy a separate DataBend instance.

## Tutorial-1: Integrate with Jupyter Notebook using SQLAlchemy

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

![Alt text](../../public/img/integration/notebook-tree.png)

2. On the **Files** tab, navigate to the sample notebook you downloaded and open it.

3. In the sample notebook, run the cells sequentially. By doing so, you create a table containing 5 rows in your local Databend, and visualize the data with a bar chart.

![Alt text](../../public/img/integration/integration-gui-jupyter.png)

## Tutorial-2: Integrate with Jupyter Notebook using ipython-sql

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

:::note
To proceed with this tutorial, you'll need a version of SQLAlchemy that is below 2.0. Please be aware that in SQLAlchemy 2.0 and later versions, the result.DataFrame() method has been deprecated and is no longer available. Instead, you can use the pandas library to directly create a DataFrame from query results and perform plotting.
:::

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

![Alt text](../../public/img/integration/notebook-tree.png)

2. Select **New** > **Python 3** to create a notebook.

3. Run the following code sequentially in separate cells. By doing so, you create a table containing 5 rows in your local Databend, and visualize the data with a bar chart.

```python title='In [1]:'
%load_ext sql
```

```sql title='In [2]:'
%%sql databend://user1:abc123@localhost:8000/default
create table if not exists user(created_at Date, count Int32);
insert into user values('2022-04-01', 5);
insert into user values('2022-04-01', 3);
insert into user values('2022-04-03', 4);
insert into user values('2022-04-03', 1);
insert into user values('2022-04-04', 10);
```

```python title='In [3]:'
result = %sql select created_at as date, count(*) as count from user group by created_at;
result
```

```python title='In [4]:'
%matplotlib inline

df = result.DataFrame()
df.plot.bar(x='date', y='count')
```

You can now see a bar chart on the notebook:

![Alt text](../../public/img/integration/jupyter-ipython-sql.png)

## Tutorial-3: Integrate with Jupyter Notebook with Python Binding Library

In this tutorial, you will first deploy a local Databend instance and Jupyter Notebook, and then run queries in a notebook through the [Databend Python Binding](https://pypi.org/project/databend/) library, as well as write and visualize data within the notebook.

Before you start, ensure that you have [Python](https://www.python.org/) installed on your system.

### Step 1. Deploy Jupyter Notebook

1. Install Jupyter Notebook with pip:

```shell
pip install notebook
```

2. Install dependencies with pip:

```shell
pip install databend
pip install matplotlib
```
### Step 2. Create a Notebook

1. Run the command below to start Jupyter Notebook:

```shell
jupyter notebook
```

  This will start up Jupyter and your default browser should start (or open a new tab) to the following URL: http://localhost:8888/tree

![Alt text](../../public/img/integration/notebook-tree.png)

2. Select **New** > **Python 3** to create a notebook.

3. Run the following code sequentially in separate cells:

```python title='In [1]:'
# Import the necessary libraries
from databend import SessionContext

# Create a DataBend session
ctx = SessionContext()
```

```python title='In [2]:'
# Create a table in DataBend
ctx.sql("CREATE TABLE IF NOT EXISTS user (created_at Date, count Int32)")
```

```python title='In [3]:'
# Insert multiple rows of data into the table
ctx.sql("INSERT INTO user VALUES ('2022-04-01', 5), ('2022-04-01', 3), ('2022-04-03', 4), ('2022-04-03', 1), ('2022-04-04', 10)")
```

```python title='In [4]:'
# Execute a query
result = ctx.sql("SELECT created_at as date, count(*) as count FROM user GROUP BY created_at")

# Display the query result
result.show()
```

```python title='In [5]:'
# Import libraries for data visualization
import matplotlib.pyplot as plt

# Convert the query result to a Pandas DataFrame
df = result.to_pandas()
```

```python title='In [6]:'
# Create a bar chart to visualize the data
df.plot.bar(x='date', y='count')
plt.show()
```

You can now see a bar chart on the notebook:

![Alt text](../../public/img/integration/localhost_8888_notebooks_Untitled.ipynb.png)