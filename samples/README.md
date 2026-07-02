# Databend Sample Notebooks

Interactive Jupyter notebooks to help you get started with Databend.

## Prerequisites

- Python 3.12 or 3.13
- Jupyter Notebook or JupyterLab

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start Jupyter
jupyter notebook
```

Or use Databend Cloud directly (no local setup needed):

```bash
pip install databend-driver
```

## Notebooks

### Getting Started
| Notebook | Description |
|----------|-------------|
| [01_connect.ipynb](00_getting_started/01_connect.ipynb) | Connect to Databend (local and cloud) |
| [02_basic_sql.ipynb](00_getting_started/02_basic_sql.ipynb) | CREATE TABLE, INSERT, SELECT, JOIN |

### Analytics
| Notebook | Description |
|----------|-------------|
| [03_analytics_basics.ipynb](01_analytics/03_analytics_basics.ipynb) | Window functions, CTEs, aggregations |

### Vector Search
| Notebook | Description |
|----------|-------------|
| [04_vector_search.ipynb](02_vector_search/04_vector_search.ipynb) | Vector columns, similarity search |

### Python UDF
| Notebook | Description |
|----------|-------------|
| [05_sandbox_udf.ipynb](03_python_udf/05_sandbox_udf.ipynb) | Create Python UDFs, agent orchestration |

## Resources

- [Databend Documentation](https://docs.databend.com)
- [Databend Cloud](https://app.databend.com) (free trial)
- [Python Driver API](https://github.com/datafuselabs/databend-python)
