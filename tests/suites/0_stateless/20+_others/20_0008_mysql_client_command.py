#!/usr/bin/env python3

import os
import sys
import time

# test mysql client command, client command status will return these value.
# We need promise that the command can execute and keep the connection.
# mysql> status;
# --------------
# mysql  Ver 8.0.29-0ubuntu0.20.04.3 for Linux on x86_64 ((Ubuntu))
#
# Connection id:		41
# Current database:	default
# Current user:		'root'@'%'
# SSL:			Not in use
# Current pager:		less
# Using outfile:		''
# Using delimiter:	;
# Server version:		8.0.26-v0.7.73-nightly-49c6aff-simd(rust-1.63.0-nightly-2022-06-14T01:56:14.925665572Z) 0
# Protocol version:	10
# Connection:		127.0.0.1 via TCP/IP
# Server characterset:	0
# Db     characterset:	0
# Client characterset:	0
# Conn.  characterset:	0
# TCP port:		3307
# Binary data as:		Hexadecimal
# --------------

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from client import client

log = None
# uncomment the line below for debugging
log = sys.stdout

client1 = client(name="client1>", log=log)

stdout, stderr = client1.run_with_output("status;select 'client_command_test';")
assert "3307" in stdout
assert "client_command_test" in stdout
assert stderr is None
