---
title: MASKING POLICY
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';
import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='MASKING POLICY'/>

### What is Masking Policy?

A masking policy refers to rules and settings that control the display or access to sensitive data in a way that safeguards confidentiality while allowing authorized users to interact with the data. Databend enables you to define masking policies for displaying sensitive columns in a table, thus protecting confidential data while still permitting authorized roles to access specific parts of the data.

To illustrate, consider a scenario where you want to present email addresses in a table exclusively to managers:

```sql
id | email           |
---|-----------------|
 2 | eric@example.com|
 1 | sue@example.com |
```

And when non-manager users query the table, the email addresses would appear as:

```sql
id|email    |
--+---------+
 2|*********|
 1|*********|
```

### Implementing Masking Policy

Before creating a masking policy, make sure you have properly defined or planned user roles and their corresponding access privileges, as the policy's implementation relies on these roles to ensure secure and effective data masking. To manage Databend users and roles, refer to **SQL Commands** > **DDL Commands** > **User**.

Masking policies are applied to the columns of a table. To implement a masking policy for a specific column, you must first create the masking policy and then associate the policy to the intended column with the [ALTER TABLE COLUMN](../20-table/90-alter-table-column.md) command. By establishing this association, the masking policy becomes tailored to the exact context where data privacy is paramount. It's important to note that a single masking policy can be associated with multiple columns, as long as they align with the same policy criteria. To manage masking policies in Databend, use the following commands:

<IndexOverviewList />

### Usage Example

This example illustrates the process of setting up a masking policy to selectively reveal or mask sensitive data based on user roles.

```sql
-- Create a table and insert sample data
CREATE TABLE user_info (
    id INT,
    email STRING
);

INSERT INTO user_info (id, email) VALUES (1, 'sue@example.com');
INSERT INTO user_info (id, email) VALUES (2, 'eric@example.com');

-- Create a role
CREATE ROLE 'MANAGERS';
GRANT ALL ON *.* TO ROLE 'MANAGERS';

-- Create a user and grant the role to the user
CREATE USER manager_user IDENTIFIED BY 'databend';
GRANT ROLE 'MANAGERS' TO 'manager_user';

-- Create a masking policy
CREATE MASKING POLICY email_mask
AS
  (val string)
  RETURNS string ->
  CASE
  WHEN current_role() IN ('MANAGERS') THEN
    val
  ELSE
    '*********'
  END
  COMMENT = 'hide_email';

-- Associate the masking policy with the 'email' column
ALTER TABLE user_info MODIFY COLUMN email SET MASKING POLICY email_mask;

-- Query with the Root user
SELECT * FROM user_info;

id|email    |
--+---------+
 2|*********|
 1|*********|
```