---
title: Tableau
---

[Tableau](https://www.tableau.com/) is a visual analytics platform transforming the way we use data to solve problems—empowering people and organizations to make the most of their data. By leveraging the [databend-jdbc driver](https://github.com/databendcloud/databend-jdbc) (version 0.0.8 or higher), Databend seamlessly integrates with Tableau, enabling seamless data access and efficient analysis. It is important to note that for optimal compatibility, it is advisable to use Tableau version 2023.1.0 or higher to avoid potential compatibility issues. 

In the following tutorial, you will find a detailed, step-by-step guide on deploying and integrating Tableau Desktop with Databend.

## Tutorial: Integrate with Tableau Desktop

In this tutorial, you'll deploy and integrate a local Databend with [Tableau Desktop](https://www.tableau.com/products/desktop). Before you start, [download](https://www.tableau.com/products/desktop/download) Tableau Desktop and follow the on-screen instructions to complete the installation. 

### Step 1. Deploy Databend

1. Follow the [Local and Docker Deployments](../../10-deploy/05-deploying-local.md) guide to deploy a local Databend.
2. Create a SQL user in Databend. You will use this account to connect to Databend in Tableau Desktop.

```sql
CREATE USER tableau IDENTIFIED BY 'tableau';
GRANT ALL ON *.* TO tableau;
```

### Step 2. Install databend-jdbc

1. Download the databend-jdbc driver (version 0.0.8 or higher) from the Maven Central Repository at https://repo1.maven.org/maven2/com/databend/databend-jdbc/

2. To install the databend-jdbc driver, move the jar file (for example, databend-jdbc-0.0.8.jar) to Tableau's driver folder. Tableau's driver folder varies depending on the operating system:

| Operating System 	| Tableau's Driver Folder          	|
|------------------	|----------------------------------	|
| MacOS             | ~/Library/Tableau/Drivers        	|
| Windows          	| C:\Program Files\Tableau\Drivers 	|
| Linux            	| /opt/tableau/tableau_driver/jdbc 	|

### Step 3. Connect to Databend

1. Launch Tableau Desktop and select **Other Database (JDBC)** in the sidebar. This opens a window as follows:

![Alt text](../../public/img/integration/tableau-1.png)

2. In the window, provide the connection information and click **Sign In**.

| Parameter 	| Description                                                        	| For This Tutorial                                         	|
|-----------	|--------------------------------------------------------------------	|----------------------------------------------------------	|
| URL       	| Format: jdbc:databend://{user}:{password}@{host}:{port}/{database} 	| jdbc:databend://tableau:tableau@127.0.0.1:8000/default 	|
| Dialect   	| Select "MySQL" for SQL dialect.                                    	| MySQL                                                    	|
| Username  	| SQL user for connecting to Databend                                	| tableau                                                  	|
| Password  	| SQL user for connecting to Databend                                	| tableau                                                  	|

3. When the Tableau workbook opens, select the database, schema, and tables that you want to query. For this tutorial, select *default* for both **Database** and **Schema**.

![Alt text](../../public/img/integration/tableau-2.png)

You're all set! You can now drag tables to the work area to start your query and further analysis.