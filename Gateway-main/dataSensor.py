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


def store_sData() -> None:
    # connect database
    db = SqliteDAO(dbName)
    # connect mqtt
    topic = []
    topic.append(thingTopics["sensor_data"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            msg = json.loads(msg)
            msg["info"]["time"] = utils.now()
            if (msg["operator"] == "sensor_data"):
                if (utils.dataFilter(msg)):
                    # insert data
                    db.insertOneRecord("SensorMonitor",
                                    ["node_id", "co2", "temp", "hum", "light", "dust", "sound", "red", "green", "blue", "motion", "time"],
                                        utils.infoJsonToColValues(msg, ["node_id", "co2", "temp", "hum", "light", "dust", "sound", "red", "green", "blue", "motion", "time"]))

    client.loop_stop()

def store_eData() -> None:
    # connect database
    db = SqliteDAO(dbName)
    # connect mqtt
    topic = []
    topic.append(thingTopics["energy_data"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            msg = json.loads(msg)
            msg["info"]["time"] = utils.now()
            if (msg["operator"] == "energy_data"):
                if (utils.dataFilter(msg)):
                    # insert data
                    db.insertOneRecord("EnergyMonitor",
                                    ["node_id", "voltage", "current", "active_power", "power_factor", "frequency", "active_energy", "time"],
                                        utils.infoJsonToColValues(msg, ["node_id",  "voltage", "current", "active_power", "power_factor", "frequency", "active_energy", "time"]))

    client.loop_stop()

def publish_sData():
    # connect database
    db = SqliteDAO(dbName)
    # connect mqtt
    topic = serverTopics["data_request"]
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while (True):
        # list activate node
        nodeIDList = []
        for item in db.__do__(f"SELECT * FROM Registration WHERE synchronization_state = 'synchronized'"):
            print(item)
            if (utils.now() - item[4] < 60*60):
                nodeIDList.append(item[0])
        # print(nodeIDList)
        if (nodeIDList):
            for node_id in nodeIDList:
                data = utils.getAverageData(
                    dbName, node_id,  "SensorMonitor", ["co2", "temp", "hum", "light", "sound", "dust", "red", "green", "blue", "motion"], 10)
                if (data):
                    sensorDataMsg = {
                        "operator": "data_response",
                        "status": 0,
                        "info": {
                            "room_id": roomID,
                            "node_id": node_id,
                            "co2": data["co2"],
                            "temp": data["temp"],
                            "hum": data["hum"],
                            "light": data["light"],
                            "sound": data["sound"],
                            "dust": data["dust"],
                            "red": data["red"],
                            "green": data["green"],
                            "blue": data["blue"],
                            "motion": data["motion"],
                            "time": utils.now(),
                        }
                    }
                    client.publish(topic, json.dumps(sensorDataMsg))
                    print(f"PUBLISH {[item for item in data.values()]} for node {node_id} at {utils.now()}")
                    clock.sleep(2)
        clock.sleep(120)
    client.loop_stop()

def publish_eData():
    # connect database
    db = SqliteDAO(dbName)
    # connect mqtt
    topic = serverTopics["energy_data"]
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while (True):
        # list activate node
        nodeIDList = [404]
        # for item in db.__do__(f"SELECT * FROM Registration WHERE synchronization_state = 'synchronized'"):
        #     print("a")
        #     #print(item)
        #     if (utils.now() - item[4] < 60*60):
        #         nodeIDList.append(item[0])
        # #print(nodeIDList)
        if (nodeIDList):
            for node_id in nodeIDList:
                data = utils.getAverageData(
                    dbName, node_id,  "EnergyMonitor", ["voltage", "current", "active_power", "power_factor", "frequency", "active_energy"], 10)
                if (data):
                    sensorDataMsg = {
                        "operator": "energy_data",
                        "status": 0,
                        "info": {
                            "room_id": roomID,
                            "node_id": node_id,
                            "voltage": data["voltage"],
                            "current": data["current"],
                            "active_power":data["active_power"],
                            "power_factor": data["power_factor"],
                            "frequency": data["frequency"],
                            "active_energy": data["active_energy"],
                            "time": utils.now(),
                        }
                    }
                    client.publish(topic, json.dumps(sensorDataMsg))
                    print(f"PUBLISH {[item for item in data.values()]} for node {node_id} at {utils.now()}")
                    clock.sleep(2)
        clock.sleep(120)
    client.loop_stop()