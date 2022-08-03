#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

SRC_DIR=$(git rev-parse --show-toplevel)
cd $SRC_DIR

if [ -f /.dockerenv ]; then
    export PULSAR_DIR=/tmp/pulsar-test-dist
else
    export PULSAR_DIR=$SRC_DIR
fi

usage() {
    echo "Usage: $0 [start|stop] <broker-id>"
    exit 1
}

if [[ $# -lt 2 ]]; then
    usage
fi

COMMAND=$1
BROKER_ID=$2

# See ./test-conf/broker-$2.conf for the http port
if [[ $BROKER_ID == "0" ]]; then
    HTTP_PORT=8080
elif [[ $BROKER_ID == "1" ]]; then
    HTTP_PORT=8180
else
    echo "Invalid broker id $BROKER_ID";
    usage
fi
BROKER_NAME=broker-$BROKER_ID

if [[ $COMMAND == "start" ]]; then
    PULSAR_PID_DIR=$PULSAR_DIR/logs/$BROKER_NAME \
    PULSAR_LOG_DIR=$PULSAR_DIR/logs/$BROKER_NAME \
    PULSAR_BROKER_CONF=$SRC_DIR/pulsar-client-cpp/test-conf/$BROKER_NAME.conf \
        $PULSAR_DIR/bin/pulsar-daemon start broker
    until curl http://localhost:$HTTP_PORT/metrics > /dev/null 2>&1 ; do sleep 1; done
elif [[ $COMMAND == "stop" ]]; then
    PULSAR_PID_DIR=$PULSAR_DIR/logs/$BROKER_NAME \
        $PULSAR_DIR/bin/pulsar-daemon stop broker
else
    echo "Unknown command: $2"
    usage
fi
