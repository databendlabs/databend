#!/usr/bin/env python3

import os
import sys
import signal

import boto3
from moto import mock_s3

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, '../../../helpers'))

from client import client

log = None
# uncomment the line below for debugging
log = sys.stdout

client1 = client(name='client1>', log=log)

sqls = """
select 1;
select 2;
select 3;
"""

client1.run(sqls)
client1.run("select 1")
client1.run("select 2")
client1.run("select 3")


class MyModel(object):

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def save(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket='mybucket', Key=self.name, Body=self.value)


@mock_s3
def test_my_model_save():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket='mybucket')

    model_instance = MyModel('steve', 'is awesome')
    model_instance.save()

    body = conn.Object('mybucket',
                       'steve').get()['Body'].read().decode("utf-8")

    assert body == 'is awesome'


test_my_model_save()
