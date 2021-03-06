# Copyright 2017 Intel Research and Development Ireland Limited
# *************************************************************
# This file is part of OSM Monitoring module
# All Rights Reserved to Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You may
# obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# For those usages not covered by the Apache License, Version 2.0 please
# contact: helena.mcgough@intel.com or adrian.hoban@intel.com
"""A common KafkaConsumer for all MON plugins."""

import json
import logging
import sys
import os

sys.path.append("/root/MON")
sys.path.append("../../plugins/CloudWatch")

logging.basicConfig(filename='MON_plugins.log',
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', filemode='a',
                    level=logging.INFO)
log = logging.getLogger(__name__)

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from osm_mon.plugins.OpenStack.Aodh import alarming
from osm_mon.plugins.OpenStack.common import Common
from osm_mon.plugins.OpenStack.Gnocchi import metrics

from plugin_alarm import plugin_alarms
from plugin_metric import plugin_metrics

# Initialize servers
server = {'server': 'localhost:9092'}

# Initialize consumers for alarms and metrics
common_consumer = KafkaConsumer(bootstrap_servers=server['server'])

# Create OpenStack alarming and metric instances
auth_token = None
openstack_auth = Common()
openstack_metrics = metrics.Metrics()
openstack_alarms = alarming.Alarming()

# Create CloudWatch alarm and metric instances
cloudwatch_alarms = plugin_alarms()
cloudwatch_metrics = plugin_metrics()

def get_vim_type(message):
    """Get the vim type that is required by the message."""
    try:
        return json.loads(message.value)["vim_type"].lower()
    except Exception as exc:
        log.warn("vim_type is not configured correctly; %s", exc)
    return None

# Define subscribe the consumer for the plugins
topics = ['metric_request', 'alarm_request', 'access_credentials']
common_consumer.subscribe(topics)

try:
    log.info("Listening for alarm_request and metric_request messages")
    for message in common_consumer:
        # Check the message topic
        if message.topic == "metric_request":
            # Check the vim desired by the message
            vim_type = get_vim_type(message)
            
            if vim_type == "openstack":
                log.info("This message is for the OpenStack plugin.")
                openstack_metrics.metric_calls(
                    message, openstack_auth, auth_token)

            elif vim_type == "aws":
                cloudwatch_metrics.metric_calls(message)
                log.info("This message is for the CloudWatch plugin.")

            elif vim_type == "vrops":
                log.info("This message is for the vROPs plugin.")

            else:   
                log.debug("vim_type is misconfigured or unsupported; %s",
                          vim_type)

        elif message.topic == "alarm_request":
            # Check the vim desired by the message
            vim_type = get_vim_type(message)
            if vim_type == "openstack":
                log.info("This message is for the OpenStack plugin.")
                openstack_alarms.alarming(message, openstack_auth, auth_token)

            elif vim_type == "aws":
                cloudwatch_alarms.alarm_calls(message)
                log.info("This message is for the CloudWatch plugin.")

            elif vim_type == "vrops":
                log.info("This message is for the vROPs plugin.")

            else:
                log.debug("vim_type is misconfigured or unsupported; %s",
                          vim_type)

        elif message.topic == "access_credentials":
            # Check the vim desired by the message
            vim_type = get_vim_type(message)
            if vim_type == "openstack":
                log.info("This message is for the OpenStack plugin.")
                auth_token = openstack_auth._authenticate(message=message)

            elif vim_type == "aws":
                #TODO Access credentials later
                log.info("This message is for the CloudWatch plugin.")

            elif vim_type == "vrops":
                log.info("This message is for the vROPs plugin.")

            else:
                log.debug("vim_type is misconfigured or unsupported; %s",
                          vim_type)

        else:
            log.info("This topic is not relevant to any of the MON plugins.")


except KafkaError as exc:
    log.warn("Exception: %s", exc)
