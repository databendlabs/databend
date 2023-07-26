---
title: dbt
---

[dbt](https://www.getdbt.com/) is a transformation workflow that helps you get more work done while producing higher quality results. You can use dbt to modularize and centralize your analytics code, while also providing your data team with guardrails typically found in software engineering workflows. Collaborate on data models, version them, and test and document your queries before safely deploying them to production, with monitoring and visibility.

[dbt-databend-cloud](https://github.com/databendcloud/dbt-databend) is a plugin developed by Databend with the primary goal of enabling smooth integration between dbt and Databend. By utilizing this plugin, you can seamlessly perform data modeling, transformation, and cleansing tasks using dbt and conveniently load the output into Databend. The table below illustrates the level of support that the dbt-databend-cloud plugin offers for commonly used features in dbt:

| Feature                     	| Supported ? |
|-----------------------------	|-----------	|
| Table Materialization       	| Yes       	|
| View Materialization        	| Yes       	|
| Incremental Materialization 	| Yes       	|
| Ephemeral Materialization   	| No        	|
| Seeds                       	| Yes       	|
| Sources                     	| Yes       	|
| Custom Data Tests           	| Yes       	|
| Docs Generate               	| Yes       	|
| Snapshots                   	| No        	|
| Connection Retry            	| Yes       	|

## Installing dbt-databend-cloud

Installing the dbt-databend-cloud plugin has been streamlined for your convenience, as it now includes dbt as a required dependency. To effortlessly set up both dbt and the dbt-databend-cloud plugin, run the following command:

```shell
pip3 install dbt-databend-cloud
```

However, if you prefer to install dbt separately, you can refer to the official dbt installation guide for detailed instructions.

## Tutorial: Run dbt Project jaffle_shop

If you're new to dbt, Databend recommends completing the official dbt tutorial available at https://github.com/dbt-labs/jaffle_shop. Before you start, follow [Installing dbt-databend-cloud](#installing-dbt-databend-cloud) to install dbt and dbt-databend-cloud.

This tutorial provides a sample dbt project called "jaffle_shop," offering hands-on experience with the dbt tool. By configuring the default global profile (~/.dbt/profiles.yml) with the necessary information to connect to your Databend instance, the project will generate tables and views defined in the dbt models directly in your Databend database. Here's an example of the file profiles.yml that connects to a local Databend instance:

```yml title="~/.dbt/profiles.yml"
jaffle_shop_databend:
  target: dev
  outputs:
    dev:
      type: databend
      host: 127.0.0.1
      port: 8000
      schema: sjh_dbt
      user: databend
      pass: ********
```

If you're using Databend Cloud, you can refer to this [Wiki page](https://github.com/databendcloud/dbt-databend/wiki/How-to-use-dbt-with-Databend-Cloud) for step-by-step instructions on how to run the jaffle_shop dbt project.