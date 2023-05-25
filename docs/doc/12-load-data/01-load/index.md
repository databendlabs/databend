---
title: Loading Data into Databend
slug: ./
---

Databend provides a variety of tools and commands that can help you load your data files into a table. Most of them are straightforward, meaning you can load your data with just a single command. Please note that your data files must be in one of the formats supported by Databend. See [Input & Output File Formats](../../13-sql-reference/50-file-format-options.md) for a list of supported file formats.

This topic does not cover all of the available data loading methods, but it provides recommendations based on the location where your data files are stored. To find the recommended method and a link to the corresponding details page, toggle the block below:

<details>
  <summary>I want to load staged data files ...</summary>
  <div>
    <div>If you have data files in an internal/external stage or the user stage, Databend recommends that you load them using the COPY INTO command. The COPY INTO command is a powerful tool that can load large amounts of data quickly and efficiently.</div>
    <br/>
    <div>To learn more about using the COPY INTO command to load data from a stage, check out the <a href="stage">Loading from Stage</a> page. This page includes detailed tutorials that show you how to use the command to load data from a sample file in an internal/external stage or the user stage.</div>
  </div>
</details>

<details>
  <summary>I want to load data files in a bucket ...</summary>
  <div>
    <div>If you have data files in a bucket or container on your object storage, such as Amazon S3, Google Cloud Storage, and Microsoft Azure, Databend recommends that you load them using the COPY INTO command. The COPY INTO command is a powerful tool that can load large amounts of data quickly and efficiently.</div>
    <br/>
    <div>To learn more about using the COPY INTO command to load data from a bucket or container, check out the <a href="s3">Loading from Bucket</a> page. This page includes a tutorial that shows you how to use the command to load data from a sample file in an Amazon S3 Bucket.</div>
  </div>
</details>

<details>
  <summary>I want to load local data files ...</summary>
  <div>
    <div>If you have data files in your local system, Databend recommends that you load them using <a href="https://github.com/datafuselabs/BendSQL">BendSQL</a>, the Databend native CLI tool, allowing you to establish a connection with Databend and execute queries directly from a CLI window.</div>
    <br/>
    <div>To learn more about using BendSQL to load your local data files, check out the <a href="local">Loading from Local File</a> page. This page includes tutorials that show you how to use the tool to load data from a local sample file.</div>
  </div>
</details>

<details>
  <summary>I want to load remote data files ...</summary>
  <div>
    <div>If you have remote data files, Databend recommends that you load them using the COPY INTO command. The COPY INTO command is a powerful tool that can load large amounts of data quickly and efficiently.</div>
    <br/>
    <div>To learn more about using the COPY INTO command to load remote data files, check out the <a href="http">Loading from Remote File</a> page. This page includes a tutorial that shows you how to use the command to load data from a remote sample file.</div>
  </div>
</details>