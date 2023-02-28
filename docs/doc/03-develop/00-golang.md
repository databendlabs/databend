---
title: Developing with Databend using Golang
sidebar_label: Golang
description:
   How to Develop with Databend using Golang.
---

Databend offers a driver (databend-go) written in Golang, which facilitates the development of applications using the Golang programming language and establishes connectivity with Databend.

For installation instructions, examples, and the source code, see the GitHub [databend-go](https://github.com/databendcloud/databend-go) repo.

In the following tutorial, you'll learn how to utilize the driver `databend-go` to develop your applications. The tutorial will walk you through creating a SQL user in Databend and then writing Golang code to create a table, insert data, and perform data queries.


## Tutorial: Developing with Databend using Golang

Before you start, make sure you have successfully installed Databend. For how to install Databend, see [How to deploy Databend](/doc/deploy).

## Step 1. Create a SQL User

In this step, you'll create a SQL user in Databend and grant privileges to the user.

1. Connect to Databend from a MySQL client.

```shell
mysql -h127.0.0.1 -uroot -P3307
```

2. Create a user named `user1`.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

3. Grant privileges to `user1`.

```sql
GRANT ALL on *.* TO user1;
```

## Step 2. Write a Golang Program

In this step, you'll create a simple Golang program that communicates with Databend. The program will involve tasks such as creating a table, inserting data, and executing data queries.

1. Copy and paste the following code to the file `main.go`:

```go title='main.go'
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/databendcloud/databend-go"
)

const (
	username = "user1"
	password = "abc123"
	hostname = "127.0.0.1:8000"
)

type Book struct {
	Title  string
	Author string
	Date   string
}

func dsn() string {
	return fmt.Sprintf("http://%s:%s@%s", username, password, hostname)
}

func main() {
	db, err := sql.Open("databend", dsn())

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected")

	// Create db if do not exist
	dbSql := "CREATE DATABASE IF NOT EXISTS book_db"
	_, err = db.Exec(dbSql)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create database book_db success")

	// Use book_db database
	_, err = db.Exec("USE book_db")
	if err != nil {
		log.Fatal(err)
	}

	// Create table.
	sql := "create table if not exists books(title VARCHAR, author VARCHAR, date VARCHAR)"
	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create table: books")

	// Insert 1 row.
	_, err = db.Exec("INSERT INTO books VALUES(?, ?, ?)", "mybook", "author", "2022")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Insert 1 row")

	// Select.
	res, err := db.Query("SELECT * FROM books")
	if err != nil {
		log.Fatal(err)
	}

	for res.Next() {
		var book Book
		err := res.Scan(&book.Title, &book.Author, &book.Date)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Select:%v", book)
	}
	db.Exec("drop table books")
	db.Exec("drop database book_db")
}
```

2. Install dependencies. 

```shell
go mod init databend-golang
```

```text title='go.mod'
module databend-golang

go 1.20

require github.com/databendcloud/databend-go v0.3.10

require (
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/avast/retry-go v3.0.0+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
)
```

3. Run the program. 

```shell
go run main.go
```

```text title='Outputs'
2023/02/24 23:57:31 Connected
2023/02/24 23:57:31 Create database book_db success
2023/02/24 23:57:31 Create table: books
2023/02/24 23:57:31 Insert 1 row
2023/02/24 23:57:31 Select:{mybook author 2022}
```