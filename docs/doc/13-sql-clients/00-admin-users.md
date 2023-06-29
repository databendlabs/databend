---
title: Configuring Admin Users
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.75"/>

Databend doesn't provide any built-in admin users out-of-the-box. Before the initial startup of Databend, it is necessary to configure one in the *databend-query.toml* configuration file, which is equivalent to the root user in other databases. To do so, follow these steps:

1. Open the *databend-query.toml* configuration file, then locate the [query.users] section.

2. Uncomment (remove the # symbol) the user accounts you want to use or add your own ones in the same format. For each user, specify the following information:
    - **name**: The username for the account.
    - **auth_type**: The authentication type for the account. It can be either "no_password", "double_sha1_password", or "sha256_password".
    - **auth_string**: The password or authentication string associated with the user account.

To generate the **auth_string**, use cryptographic hash functions. Here's how you can generate the auth_string for each authentication type mentioned:

- **no_password**: For the no_password authentication type, no password is required. In this case, the auth_string field is not needed at all.

- **double_sha1_password**: To generate the auth_string for the double_sha1_password authentication type, choose a password first (e.g., "databend"). Then, run the following command and use the resulting output as the auth_string:

  ```shell
  echo -n "databend" | sha1sum | cut -d' ' -f1 | xxd -r -p | sha1sum
  ```

- **sha256_password**: To generate the auth_string for the sha256_password authentication type, choose a password first (e.g., "databend"). Then, run the following command and use the resulting output as the auth_string:

  ```shell
  echo -n "databend" | sha256sum
  ```