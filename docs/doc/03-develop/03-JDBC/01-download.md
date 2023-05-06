---
title: Downloading / Integrating the JDBC Driver
sidebar_label: Download 
description:
  Downloading / Integrating the JDBC Driver
---

The JDBC driver (databend-jdbc) is provided as a JAR file, available as an artifact in Maven for download or integrating directly into your Java-based projects.

## Downloading the Driver
To download the driver:

1. Go to the Maven Central Repository:
https://repo1.maven.org/maven2/com/databend/databend-jdbc/
2. Click on the directory for the version that you need.
3. Download the databend-jdbc-#.#.#.jar file.

> Note:
> 
>If desired, you can verify the JDBC driver version by entering the following command:
java -jar databend-jdbc-#.#.#.jar --version, where #.#.# matches the version numbers in the downloaded file name.
If you plan to verify the driver package signature, download the databend-jdbc-#.#.#.jar.asc file.