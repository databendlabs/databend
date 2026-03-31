<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Setup GPG key

> This section is a brief from the [Cryptography with OpenPGP](https://infra.apache.org/openpgp.html) guideline.


## Install GPG

For more details, please refer to [GPG official website](https://www.gnupg.org/download/index.html). Here shows one approach to install GPG with `apt`:

```shell
sudo apt install gnupg2
```

## Generate GPG Key

Attentions:

- Name is best to keep consistent with your full name of Apache ID;
- Email should be the Apache email;
- Name is best to only use English to avoid garbled.

Run `gpg --full-gen-key` and complete the generation interactively:

```shell
gpg (GnuPG) 2.2.20; Copyright (C) 2020 Free Software Foundation, Inc.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.

Please select what kind of key you want:
   (1) RSA and RSA (default)
   (2) DSA and Elgamal
   (3) DSA (sign only)
   (4) RSA (sign only)
  (14) Existing key from card
Your selection? 1 # input 1
RSA keys may be between 1024 and 4096 bits long.
What keysize do you want? (2048) 4096 # input 4096
Requested keysize is 4096 bits
Please specify how long the key should be valid.
         0 = key does not expire
      <n>  = key expires in n days
      <n>w = key expires in n weeks
      <n>m = key expires in n months
      <n>y = key expires in n years
Key is valid for? (0) 0 # input 0
Key does not expire at all
Is this correct? (y/N) y # input y

GnuPG needs to construct a user ID to identify your key.

Real name: Hulk Lin               # input your name
Email address: hulk@apache.org    # input your email
Comment:                          # input some annotations, optional
You selected this USER-ID:
    "Hulk <hulk@apache.org>"

Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit? O # input O
We need to generate a lot of random bytes. It is a good idea to perform
some other action (type on the keyboard, move the mouse, utilize the
disks) during the prime generation; this gives the random number
generator a better chance to gain enough entropy.
We need to generate a lot of random bytes. It is a good idea to perform
some other action (type on the keyboard, move the mouse, utilize the
disks) during the prime generation; this gives the random number
generator a better chance to gain enough entropy.

# Input the security key
┌──────────────────────────────────────────────────────┐
│ Please enter this passphrase                         │
│                                                      │
│ Passphrase: _______________________________          │
│                                                      │
│       <OK>                              <Cancel>     │
└──────────────────────────────────────────────────────┘
# key generation will be done after your inputting the key with the following output
gpg: key E49B00F626B marked as ultimately trusted
gpg: revocation certificate stored as '/Users/hulk/.gnupg/openpgp-revocs.d/F77B887A4F25A9468C513E9AA3008E49B00F626B.rev'
public and secret key created and signed.

pub   rsa4096 2022-07-12 [SC]
      F77B887A4F25A9468C513E9AA3008E49B00F626B
uid           [ultimate] hulk <hulk@apache.org>
sub   rsa4096 2022-07-12 [E]
```

## Upload your key to public GPG keyserver

Firstly, list your key:

```shell
gpg --list-keys
```

The output is like:

```shell
-------------------------------
pub   rsa4096 2022-07-12 [SC]
      F77B887A4F25A9468C513E9AA3008E49B00F626B
uid           [ultimate] hulk <hulk@apache.org>
sub   rsa4096 2022-07-12 [E]
```

Then, send your key id to key server:

```shell
gpg --keyserver keys.openpgp.org --send-key <key-id> # e.g., F77B887A4F25A9468C513E9AA3008E49B00F626B
```

Among them, `keys.openpgp.org` is a randomly selected keyserver, you can use `keyserver.ubuntu.com` or any other full-featured keyserver.

## Check whether the key is created successfully

Uploading takes about one minute; after that, you can check by your email at the corresponding keyserver.

Uploading keys to the keyserver is mainly for joining a [Web of Trust](https://infra.apache.org/release-signing.html#web-of-trust).

## Add your GPG public key to the KEYS document

:::info

`SVN` is required for this step.

:::

The svn repository of the release branch is: https://dist.apache.org/repos/dist/release/iceberg

Please always add the public key to KEYS in the release branch:

```shell
svn co https://dist.apache.org/repos/dist/release/iceberg iceberg-dist
# As this step will copy all the versions, it will take some time. If the network is broken, please use svn cleanup to delete the lock before re-execute it.
cd iceberg-dist
(gpg --list-sigs YOUR_NAME@apache.org && gpg --export --armor YOUR_NAME@apache.org) >> KEYS # Append your key to the KEYS file
svn add .   # It is not needed if the KEYS document exists before.
svn ci -m "add gpg key for YOUR_NAME" # Later on, if you are asked to enter a username and password, just use your apache username and password.
```

## Upload the GPG public key to your GitHub account

- Enter https://github.com/settings/keys to add your GPG key.
- Please remember to bind the email address used in the GPG key to your GitHub account (https://github.com/settings/emails) if you find "unverified" after adding it.
