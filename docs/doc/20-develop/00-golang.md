---
title: How to Work With Databend in Golang
sidebar_label: Golang
description:
   How to work with Databend in Golang.
---

## Before You Begin

* **Databend :** Make sure Databend is running and accessible, see [How to deploy Databend](/doc/deploy).
* [How to Create User](../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to User](../30-reference/30-sql/00-ddl/30-user/10-grant-privileges.md)
 
## Create Databend User

```shell
mysql -h127.0.0.1 -uroot -P3307
```

### Create a User 

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql
GRANT ALL on *.* TO user1;
```

## Golang

This guideline show how to connect and query to Databend using Golang. We will be creating a table named `books` and insert a row, then query it.

### main.go 

```text title='main.go'
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

const (
	username = "user1"
	password = "abc123"
	hostname = "127.0.0.1:3307"
)

type Book struct {
	Title   string
	Author string
	Date   string
}

func dsn() string {
	// Note Databend do not support prepared statements.
	// set interpolateParams to make placeholders (?) in calls to db.Query() and db.Exec() interpolated into a single query string with given parameters.
	// ref https://github.com/go-sql-driver/mysql#interpolateparams
	return fmt.Sprintf("%s:%s@tcp(%s)/?interpolateParams=true", username, password, hostname)
}

func main() {
	db, err := sql.Open("mysql", dsn())

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

}
```

### Golang mod

```text
go mod init databend-golang
```

```text title='go.mod'
module databend-golang

go 1.18

require github.com/go-sql-driver/mysql v1.6.0
```

### Run main.go

```shell
go run main.go
```

```text title='Outputs'
2022/04/13 12:20:07 Connected
2022/04/13 12:20:07 Create database book_db success
2022/04/13 12:20:07 Create table: books
2022/04/13 12:20:07 Insert 1 row
2022/04/13 12:20:08 Select:{mybook author 2022}
```
