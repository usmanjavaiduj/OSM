# Copyright 2017 Intel Research and Development Ireland Limited
# *************************************************************

# This file is part of OSM Monitoring module
# All Rights Reserved to Intel Corporation

# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at

#         http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# For those usages not covered by the Apache License, Version 2.0 please
# contact: prithiv.mohan@intel.com or adrian.hoban@intel.com
##
"""This is a KafkaProducer with a request function to test the plugins."""

import json

import logging as log

import os

import jsmin

from kafka import KafkaProducer as kaf

from kafka.errors import KafkaError


class KafkaProducer(object):
    """A KafkaProducer for testing purposes."""

    def __init__(self, topic):
        """Initialize a KafkaProducer and it's topic."""
        self._topic = topic

        if "ZOOKEEPER_URI" in os.environ:
            broker = os.getenv("ZOOKEEPER_URI")
        else:
            broker = "localhost:9092"

            '''
            If the zookeeper broker URI is not set in the env, by default,
            localhost container is taken as the host because an instance of
            is already running.
            '''

        self.producer = kaf(
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode('ascii'),
            bootstrap_servers=broker, api_version=(0, 10))

    def publish(self, key, value, topic):
        """Send messages to the message bus with a defing key and topic."""
        try:
            future = self.producer.send(topic=topic, key=key, value=value)
            self.producer.flush()
        except Exception:
            log.exception("Error publishing to {} topic." .format(topic))
            raise
        try:
            record_metadata = future.get(timeout=10)
            log.debug("TOPIC:", record_metadata.topic)
            log.debug("PARTITION:", record_metadata.partition)
            log.debug("OFFSET:", record_metadata.offset)
        except KafkaError:
            pass

    def request(self, path, key, message, topic):
        """Test json files are loaded and sent on the message bus."""
        # External to MON
        payload_create_alarm = jsmin(open(os.path.join(path)).read())
        self.publish(key=key,
                     value=json.loads(payload_create_alarm),
                     topic=topic)
