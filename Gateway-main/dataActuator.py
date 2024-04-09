from Libraly.mqtt import Client
from Libraly.dao import SqliteDAO
import json
import time as clock
import utils

dbName = utils.dbName
broker = utils.broker
port = utils.port
roomID = utils.roomID
keepalive = utils.keepalive
thingTopics = utils.thingTopics
serverTopics = utils.serverTopics


def storeData() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(thingTopics["actuator_data"])
    client = Client(topic)
    client.connect(broker, port, keepalive)  # subcribed to topics
    client.loop_start()
    while True:
        msg = client.msg_arrive()
        if (msg):
            msg = json.loads(msg)
            msg["info"]["time"] = utils.now()
            # print(msg)
            if msg["operator"] == "actuator_data":
                db.insertOneRecord("ActuatorMonitor",
                                   "node_id", "state", "speed", "time", utils.infoJsonToColValues(msg, ["node_id", "state", "speed", "time"]))
    client.loop_stop()


def pulishData():
    db = SqliteDAO(dbName)
    nodeIDList = []
    # create a client and connect that to broker
    topic = serverTopics["actuator_data"]
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while (True):
        for item in db.listAllValues("Registration"):
            if (item[0] in nodeIDList) == False:
                nodeIDList.append(item[0])
        if (nodeIDList):
            for node in nodeIDList:
                [speed, state] = utils.getAverageData(
                    dbName, node, "ActuatorMonitor", ["speed", "state"], 10)

                new_actuator_data = {
                    "operator": "actuator_data",
                    "status": 0,
                    "info":
                        {
                            "room_id": roomID,
                            "node_id": node,
                            "state": state,
                            "speed": speed,
                            "time": utils.now(),
                        }
                }
                client.publish(topic, json.dumps(new_actuator_data))
                print(f"publish {new_actuator_data} to topic '{topic}'")
                clock.sleep(2)
        clock.sleep(150)
    client.loop_stop()
