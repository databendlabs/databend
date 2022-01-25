
# Analyzing github repo with Databend

Use Databend analyzing Github repo step by step.

## Step 1 

If you already have databend ,skip it .
```
$ curl -fsS https://repo.databend.rs/databend/install-bendctl.sh | bash
$ export PATH="${HOME}/.databend/bin:${PATH}"
$ bendctl cluster create
```

## Step 2

```
mysql -h 127.0.0.1 -P 3307 -uroot

mysql> create database datafuselabs engine=github(token='<your-github-personal-access-token>');
Query OK, 0 rows affected (1.21 sec)

mysql> use datafuselabs;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+-------------------------------+---------------------------------+
| created_on                    | name                            |
+-------------------------------+---------------------------------+
| 2021-12-19 11:20:37.844 +0000 | .github                         |
| 2021-12-19 11:20:37.847 +0000 | .github_comments                |
| 2021-12-19 11:20:37.845 +0000 | .github_issues                  |
| 2021-12-19 11:20:37.846 +0000 | .github_prs                     |
| 2021-12-19 11:20:37.806 +0000 | databend                        |
| 2021-12-19 11:20:37.836 +0000 | databend-playground             |
| 2021-12-19 11:20:37.839 +0000 | databend-playground_comments    |
| 2021-12-19 11:20:37.837 +0000 | databend-playground_issues      |
| 2021-12-19 11:20:37.838 +0000 | databend-playground_prs         |
| 2021-12-19 11:20:37.814 +0000 | databend_comments               |
| 2021-12-19 11:20:37.810 +0000 | databend_issues                 |
| 2021-12-19 11:20:37.813 +0000 | databend_prs                    |
| 2021-12-19 11:20:37.852 +0000 | datafuse-operator               |
| 2021-12-19 11:20:37.855 +0000 | datafuse-operator_comments      |
| 2021-12-19 11:20:37.853 +0000 | datafuse-operator_issues        |
| 2021-12-19 11:20:37.854 +0000 | datafuse-operator_prs           |
| 2021-12-19 11:20:37.826 +0000 | datafuse-presentations          |
| 2021-12-19 11:20:37.834 +0000 | datafuse-presentations_comments |
| 2021-12-19 11:20:37.828 +0000 | datafuse-presentations_issues   |
| 2021-12-19 11:20:37.829 +0000 | datafuse-presentations_prs      |
| 2021-12-19 11:20:37.821 +0000 | datafuse-shop                   |
| 2021-12-19 11:20:37.825 +0000 | datafuse-shop_comments          |
| 2021-12-19 11:20:37.822 +0000 | datafuse-shop_issues            |
| 2021-12-19 11:20:37.823 +0000 | datafuse-shop_prs               |
| 2021-12-19 11:20:37.840 +0000 | fusebots                        |
| 2021-12-19 11:20:37.843 +0000 | fusebots_comments               |
| 2021-12-19 11:20:37.841 +0000 | fusebots_issues                 |
| 2021-12-19 11:20:37.842 +0000 | fusebots_prs                    |
| 2021-12-19 11:20:37.848 +0000 | test-infra                      |
| 2021-12-19 11:20:37.851 +0000 | test-infra_comments             |
| 2021-12-19 11:20:37.849 +0000 | test-infra_issues               |
| 2021-12-19 11:20:37.850 +0000 | test-infra_prs                  |
| 2021-12-19 11:20:37.816 +0000 | weekly                          |
| 2021-12-19 11:20:37.820 +0000 | weekly_comments                 |
| 2021-12-19 11:20:37.817 +0000 | weekly_issues                   |
| 2021-12-19 11:20:37.818 +0000 | weekly_prs                      |
+-------------------------------+---------------------------------+
36 rows in set (0.01 sec)

mysql> show create table databend_issues;
+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table           | Create Table                                                                                                                                                                                                                                                          |
+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| databend_issues | CREATE TABLE `databend_issues` (
  `number` Int64,
  `title` String,
  `state` String,
  `user` String,
  `labels` String,
  `assigness` String,
  `comments` UInt32,
  `created_at` DateTime32,
  `updated_at` DateTime32,
  `closed_at` DateTime32,
) ENGINE=GITHUB |
+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```


Q1: Query the total number of issues
```
mysql> select count(*) from databend_issues;
+----------+
| count(0) |
+----------+
|     3910 |
+----------+
1 row in set (47.00 sec)
```

Q2: Get the top 10 users who submitted issues
```
mysql> select user, count(*) as c from databend_issues group by user order by c desc limit 10;
+-----------------+------+
| user            | c    |
+-----------------+------+
| BohuTANG        | 1032 |
| drmingdrmer     |  534 |
| sundy-li        |  335 |
| dependabot[bot] |  307 |
| ZhiHanZ         |  222 |
| zhang2014       |  192 |
| dantengsky      |  187 |
| PsiACE          |  136 |
| zhyass          |   85 |
| Xuanwo          |   62 |
+-----------------+------+
10 rows in set (44.04 sec)
```

Q3:  Get the number of issue per month in 2021

```
mysql> select tomonth(created_at) as m ,count(*) as c from databend_issues where  created_at>='2021-01-01 00:00:00' and created_at<'2022-01-01 00:00:00' group by m order by m;
+------+------+
| m    | c    |
+------+------+
|    1 |   13 |
|    2 |   66 |
|    3 |  123 |
|    4 |  231 |
|    5 |  222 |
|    6 |  284 |
|    7 |  295 |
|    8 |  422 |
|    9 |  357 |
|   10 |  520 |
|   11 |  618 |
|   12 |  508 |
+------+------+
12 rows in set (46.18 sec)
``` 

Q4: Finding Comments in all Issue/Pull Request using regular expressions:

```
mysql> select * from databend_comments where body like '%expression%';
+------------+-----------+--------------------------------------------------------------------------------------------------------------------+
| comment_id | user      | body                                                                                                               |
+------------+-----------+--------------------------------------------------------------------------------------------------------------------+
|  826041283 | jyizheng  | Are you going to work on the PR of aggr expression valid check?                                                    |
|  840934322 | BohuTANG  | It seems that here need an expression action for having                                                            |
|  966089481 | junli1026 | Maybe I should move the optimization to plan_expression_common, just rewrite the expression at the very beginning. |
|  998761335 | sundy-li  | We need support lambda expression at first...                                                                      |
+------------+-----------+--------------------------------------------------------------------------------------------------------------------+
```

**Enjoy your journey.** 
