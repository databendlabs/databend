---
title: How to Work with Databend in Golang
sidebar_label: Golang
description:
   How to Work with Databend in Golang.
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

```sql title='mysql>'
create user user1 identified by 'abc123';
```

### Grants Privileges

Grants `ALL` privileges to the user `user1`:
```sql title='mysql>'
grant all on *.* to 'user1';
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
	return fmt.Sprintf("%s:%s@tcp(%s)/", username, password, hostname)
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
	dbSql := "create database if not exists book_db"
	_, err = db.Exec(dbSql)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create database book_db success")

	// Use book_db database
	_, err = db.Exec("use book_db")
	if err != nil {
		log.Fatal(err)
	}

	// Create table.
	sql := "create table if not exists books(title varchar(255), author varchar(255), date varchar(255))"
	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Create table: books")

	// Insert 1 row.
	sql = "insert into books values('mybook', 'author', '2022')"
	_, err = db.Exec(sql)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Insert 1 row")

	// Select.
	res, err := db.Query("select * from books")
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
2022/04/13 12:20:08 Select:{mybook author 2022}
```
