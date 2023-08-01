---
title: "This Week in Databend #104"
date: 2023-07-30
slug: 2023-07-30-databend-weekly
cover_url: 'weekly/weekly-104.jpg'
image: 'weekly/weekly-104.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: lichuang
  - name: nange
  - name: PsiACE
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Loading Data with Kafka

[Apache Kafka](https://kafka.apache.org/) is an open-source distributed event streaming platform that allows you to publish and subscribe to streams of records.

Databend provides an efficient data ingestion tool ([bend-ingest-kafka](https://github.com/databendcloud/bend-ingest-kafka)), specifically designed to load data from Kafka into Databend. 

If you are interested in learning more, please check out the resources listed below.

- [Docs | Loading Data with Tools - Kafka](https://databend.rs/doc/load-data/load-db/kafka)

### Loading Data with dbt

[dbt](https://www.getdbt.com/) is a transformation workflow that helps you get more work done while producing higher quality results.

[dbt-databend-cloud](https://github.com/databendcloud/dbt-databend) is a plugin developed by Databend. By utilizing this plugin, you can seamlessly perform data modeling, transformation, and cleansing tasks using dbt and conveniently load the output into Databend.  

If you are interested in learning more, please check out the resources listed below.

- [Docs | Loading Data with Tools - dbt](https://databend.rs/doc/load-data/load-db/dbt)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Developing with Databend using Node.js

Databend now provides Databend Driver Node.js Binding, which means you can easily build and integrate applications with Databend using Node.js.

```javascript
const { Client } = require('databend-driver');

const dsn = process.env.DATABEND_DSN
    ? process.env.DATABEND_DSN
    : "databend://user1:abc123@localhost:8000/default?sslmode=disable";

async function create_conn() {
    this.client = new Client(dsn);
    this.conn = await this.client.getConn();
    console.log('Connected to Databend Server!');
}

async function select_books() {
    var sql = "CREATE TABLE IF NOT EXISTS books(title VARCHAR, author VARCHAR, date VARCHAR)";
    await this.conn.exec(sql);
    console.log("Table created");

    var sql = "INSERT INTO books VALUES('Readings in Database Systems', 'Michael Stonebraker', '2004')";
    await this.conn.exec(sql);
    console.log("1 record inserted");

    var sql = "SELECT * FROM books";
    const rows = await this.conn.queryIter(sql);
    const ret = [];
    let row = await rows.next();
    while (row) {
        ret.push(row.values());
        row = await rows.next();
    }
    console.log(ret);
}

create_conn().then(conn => {
    select_books()
});
```

If you are interested in learning more, please check out the resources listed below:

- [Docs | Developing with Databend using Node.js](https://databend.rs/doc/develop/nodejs)
- [GitHub | Databend Driver Node.js Binding](https://github.com/datafuselabs/bendsql/tree/main/bindings/nodejs)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for `GROUP BY ALL`.
- Added the capability to speed up Common Table Expressions (CTEs) by materialization. 
- Added Geo functions: `h3_to_geo`, `h3_to_geo_boundary`, `h3_k_ring`, `h3_is_valid`,`h3_get_resolution`, `h3_edge_length_m` and `h3_edge_length_km`.
- Added array lambda functions: `array_transform`, `array_apply` and `array_filter`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Copying Files from One Stage to Another

Databend now supports analysis capabilities for over a dozen different storage service providers. This opens up more possibilities for data management, and copying files from one stage to another can be a beneficial starting point.

```sql
copy files from @mystage1/path/ to @mystage2;
copy files from @mystage1/path/f1.csv to @mystage2/archive/[f1.csv];
```

[Issue #12200 | Feature: support copy files from one stage to other stage](https://github.com/datafuselabs/databend/issues/12200)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.32-nightly...v1.2.42-nightly>
