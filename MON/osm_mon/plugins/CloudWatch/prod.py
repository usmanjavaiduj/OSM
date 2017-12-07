import os
import sys
sys.path.append("../../core/message_bus")
from producer import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import json
from jsmin import jsmin



producer = KafkaProducer('create_alarm_request')
#producer.create_alarm_request( 'create_alarm_request', '','alarm_request')
#producer.update_alarm_request( 'update_alarm_request', '','alarm_request')
producer.delete_alarm_request( 'delete_alarm_request', '','alarm_request')
#producer.acknowledge_alarm( 'acknowledge_alarm', '','alarm_request')
#producer.list_alarm_request( 'alarm_list_request', '','alarm_request')


server = {'server': 'localhost:9092', 'topic': 'alarm_request'}

_consumer = KafkaConsumer(bootstrap_servers=server['server'])
_consumer.subscribe(['alarm_response'])

for message in _consumer:
	print json.loads(message.value)
	


#---------------------------------------------------------------------------------

#producer = KafkaProducer('read_metric_data_request')
#producer.create_metrics_request( 'create_metric_request', '','metric_request')
#producer.read_metric_data_request( 'read_metric_data_request', '','metric_request')
#producer.update_metric_request( 'update_metric_request', '','metric_request')
#producer.delete_metric_request( 'delete_metric_request', '','metric_request')
#producer.list_metric_request( 'list_metric_request', '','metric_request')

# json_path = open(os.path.join("../../core/models/list_metric_req.json"))
# metric_info = json_path.read()
# metric_info = json.loads(metric_info)
# print metric_info

#server = {'server': 'localhost:9092', 'topic': 'metric_response'}

#_consumer = KafkaConsumer(bootstrap_servers=server['server'])
#_consumer.subscribe(['metric_response'])

#for message in _consumer:
#	print message.value



