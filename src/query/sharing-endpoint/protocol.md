# Databend Sharing protocol

The Databend Sharing Protocol is a protocol that allows tenants in a multi-tenant database system
to securely share files with each other. 

The protocol defines a set of API endpoints that can be used to manage file sharing and access control.

## Get Presigned File
### POST /tenant/{tenant_id}/{share_name}/table/{table_name}/presign
This endpoint allows a tenant to create a presigned URL for data files in a specified table. 

#### Request

```bash
POST /tenant/{tenant_id}/{share_name}/table/{table_name}/presign
```

**tenant_id** : the tenant id who shares the table
**share_name** : the share name
**table_name** : the shared table name

#### Headers

| Name | Value | Description | Required |
| ---- | ----- | ----------- |----------|
| Authorization | Bearer {token} | The token used to authenticate the request. | Yes      |
| Content-Type | application/json | The content type of the request. | No       |

#### Body

```json
[
  {
    "file_name": "file1.txt",
    "method": "GET"
  },
  {
    "file_name": "file2.txt",
    "method": "HEAD"
  }
]
```

An array of objects representing the files for which presigned URLs should be created. 

Each object should contain the following properties:
* **file_name**: The name of the file.
* **method**: The HTTP method that should be used for the presigned URL. method should be either **GET** or **HEAD**.

#### Response
```json
[
    {
        "presigned_url": "https://s3.example.com/table1/file1.txt?AWSAccessKeyId=ABC123&Expires=1560993041&Signature=def456",
        "headers": {},
        "method": "GET",
        "path": "/table1/file1.txt"
    },
    {
        "presigned_url": "https://s3.example.com/table1/file2.txt?AWSAccessKeyId=ABC123&Expires=1560993041&Signature=ghi789",
        "headers": {},
        "method": "HEAD",
        "path": "/table1/file2.txt"
    }
]

```

* **presigned_url**: The presigned URL that can be used to access the file.
* **headers**: An object containing any additional headers that should be included in the request to the presigned URL.
* **method**: The HTTP method that is allowed for the presigned URL.
* **path**: The path of the file relative to the table.
