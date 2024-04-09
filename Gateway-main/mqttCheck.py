from Libraly.mqtt import Client
from Libraly.dao import SqliteDAO
import json
import time as clock
import calendar
import datetime
import utils

dbName = utils.dbName
broker = utils.broker
port = utils.port
roomID = utils.roomID
keepalive = utils.keepalive
thingTopics = utils.thingTopics
serverTopics = utils.serverTopics

topic = []
topic.append(thingTopics["register"])
topic.append(thingTopics["register_ack"])
topic.append(thingTopics["keep_alive"])
topic.append(thingTopics["keep_alive_ack"])
topic.append(thingTopics["gateway_delete"])
topic.append(thingTopics["gateway_delete_ack"])
topic.append(thingTopics["gateway_add"])
topic.append(thingTopics["gateway_add_ack"])
topic.append(thingTopics["sensor_data"])
topic.append(thingTopics["actuator_data"])
topic.append(thingTopics["setPoint"])
topic.append(thingTopics["setpoint_ack"])
topic.append(serverTopics["data_request"])
topic.append(serverTopics["data_response"])
topic.append(serverTopics["actuator_data"])
topic.append(serverTopics["send_setpoint"])
topic.append(serverTopics["send_setpoint_ack"])
topic.append(serverTopics["server_delete"])
topic.append(serverTopics["server_delete_ack"])
topic.append(serverTopics["server_add"])
topic.append(serverTopics["server_add_ack"])
client = Client(topic)
client.connect(broker, port, keepalive) # subcribed to topics
client.loop_start()

# Msg = {
#   "operator": "server_delete",
#   "status": 1,
#   "info": {
#     "room_id": 4,
#     "mac_address": "DE:4F:22:58:96:19",
#     "time": 1655396252
#   }
# }
# client.publish(serverTopics["server_delete"], json.dumps(Msg))
# client.publish(serverTopics["server_delete"], json.dumps(Msg))
# client.publish(serverTopics["server_delete"], json.dumps(Msg))
# msg2 = {"operator": "keep_alive", "status": 0, "info": {"node_id": 2, "mac_address": "DE:4F:22:58:96:19", "time": 1700780975}}
# client.publish(thingTopics["keep_alive"], json.dumps(msg2))
# client.publish(thingTopics["keep_alive"], json.dumps(msg2))
# client.publish(thingTopics["keep_alive"], json.dumps(msg2))

# Msg = {
#   "operator": "server_add",
#   "status": 0,
#   "info": {
#     "room_id": 4,
#     "node_function":"sensor",
#     "mac_address": "DE:4F:22:58:96:69",
#     "time": 1655396252
#   }
# }
# Msg = {"operator": "server_add", "status": 0,"info": {"room_id": "4", "node_function": "air", "mac_address": "DE:4F:22:58:96:69", "time": 1701339102}}
# client.publish(serverTopics["server_add"], json.dumps(Msg))
while True:
        msg = client.msg_arrive()
        if (msg):
          print(msg)

  


  
