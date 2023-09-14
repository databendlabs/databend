---
title: SHARE ENDPOINT
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

A share endpoint, as a concept in data sharing, serves the purpose of indicating the endpoint and tenant name of the data-sharing entity. It is created within the tenant who needs to access and consume the shared data, enabling the data consumer to locate and access data shared with them.

For example, if Tenant A is sharing data with Tenant B, Tenant B needs to create a share endpoint, which will provide the necessary information, including the endpoint URL and the tenant name, enabling Tenant B to locate and access the shared data.

```sql title='Create Share Endpoint on Tenant B:'
CREATE SHARE ENDPOINT IF NOT EXISTS from_TenantA
    URL = '<share_endpoint_url>'
    TENANT = A
    COMMENT = 'Share endpoint to access data from Tenant A';
```

For a full data share workflow in Databend, see [Getting Started with Share](../90-share/index.md#getting-started-with-share). To manage share endpoints on a tenant, use the following commands:

<IndexOverviewList />