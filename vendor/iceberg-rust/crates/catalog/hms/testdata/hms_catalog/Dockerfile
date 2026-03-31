# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM apache/hive:3.1.3

ENV AWSSDK_VERSION=2.20.18
ENV HADOOP_VERSION=3.1.0

USER root

RUN apt-get update -qq && apt-get -qq -y install curl && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -Lo /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -Lo /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive
