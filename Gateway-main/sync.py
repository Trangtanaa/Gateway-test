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


# def registerProcess() -> None:
#     db = SqliteDAO(dbName)  # connect database
#     # connect mqtt
#     topic = []
#     topic.append(thingTopics["register"])
#     topic.append(thingTopics["register_ack"])
#     client = Client(topic)
#     client.connect(broker, port, keepalive)  # subcribed to topics
#     client.loop_start()
#     while True:
#         # wait to receive msg
#         msg = client.msg_arrive()
#         if (msg):
#             # convert to json format
#             msg = json.loads(msg)
#             if (msg["operator"] == "register"):
#                 # list nodes registered in database
#                 items = db.__do__(
#                     f"SELECT * FROM Registration WHERE synchronization_state = 'synchronized'")
#                 # find node
#                 for item in items:
#                     if msg["info"]["mac_address"] == item[2]:
#                         db.__do__(
#                             f"UPDATE Registration SET time = {utils.now()} WHERE node_id = {item[0]}")
#                         # create msg to send back to thing-devices
#                         registerAckMsg = {
#                             "operator": "register_ack",
#                             "status": 1,
#                             "info": {
#                                 "room_id": roomID,
#                                 "node_id": item[0],
#                                 "mac_address": msg["info"]["mac_address"],
#                                 "time": utils.now()
#                             }
#                         }
#                         client.publish(
#                             thingTopics["register_ack"], json.dumps(registerAckMsg))
#                         print(msg["info"]["mac_address"] +
#                               f" REGISTED with node_id {item[0]} at {utils.now()}")
#     client.loop_stop()


def keepAliveProcess() -> None:
    db = SqliteDAO(dbName)  # connect database
    # connect mqtt
    topic = []
    topic.append(thingTopics["keep_alive"])
    topic.append(thingTopics["keep_alive_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        for item in db.__do__(f"SELECT * FROM Registration WHERE synchronization_state = 'synchronized'"):
            if (utils.now() - item[4] > 10):
                clock.sleep(3)
                keepAliveMsg = {
                    "operator": "keep_alive",
                    "status": 0,
                    "info": {
                        "node_id": item[0],
                        "mac_address": item[2],
                        "time": utils.now()
                    }
                }
                client.publish(
                    thingTopics["keep_alive"], json.dumps(keepAliveMsg))
        clock.sleep(60)
    client.loop_stop()


def keepAliveAckProcess() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(thingTopics["keep_alive_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            msg = json.loads(msg)
            # if receive msg keepAliveAck, update node's timestamp
            if (msg["operator"] == "keep_alive_ack"):
                id = msg["info"]["node_id"]
                db.__do__(
                    f"UPDATE Registration SET time = {utils.now()} WHERE node_id = {id}")
                print(f"KEEP ALIVE node {id} at {utils.now()}")
    client.loop_stop()


def serverDeleteProcess() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(thingTopics["gateway_delete"])
    topic.append(thingTopics["gateway_delete_ack"])
    topic.append(serverTopics["server_delete"])
    topic.append(serverTopics["server_delete_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            msg = json.loads(msg)
            macAddress = msg["info"]["mac_address"]
            
            if (msg["operator"] == "server_delete" and msg["info"]["room_id"] == roomID):
                if (msg["info"]["mac_address"] in db.listAllValuesInColumn("Registration", "mac_address")):
                    db.__do__(
                        f"UPDATE Registration SET synchronization_state = 'gateway_delete' WHERE mac_address = '{macAddress}'")
                else:
                    msgServerDeleteAck = {
                        "operator": "server_delete_ack",
                        "status": 1,
                        "info": {
                            "room_id": roomID,
                            "mac_address": macAddress,
                            "time": utils.now()
                        }
                    }
                    client.publish(
                        serverTopics["server_delete_ack"], json.dumps(msgServerDeleteAck))
                    print(f"DELETE {macAddress} at {utils.now()}")

            if (msg["operator"] == "gateway_delete_ack"):
                db.__do__(
                    f"UPDATE Registration SET synchronization_state = 'deleted' WHERE mac_address = '{macAddress}'")
                msgServerDeleteAck = {
                    "operator": "server_delete_ack",
                    "status": 1,
                    "info": {
                        "room_id": roomID,
                        "mac_address": macAddress,
                        "time": utils.now()
                    }
                }
                client.publish(
                    serverTopics["server_delete_ack"], json.dumps(msgServerDeleteAck))
                print(f"DELETE {macAddress} at {utils.now()}")


def gatewayDeleteProcess() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(thingTopics["gateway_delete"])
    topic.append(thingTopics["gateway_delete_ack"])
    topic.append(serverTopics["server_delete"])
    topic.append(serverTopics["server_delete_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while (True):
        items = db.__do__(
            f"SELECT * FROM Registration WHERE synchronization_state = 'gateway_delete'")
        for item in items:
            time = item[4]
            macAddress = item[2]
            if (utils.now() - time > 120):
                db.__do__(
                    f"UPDATE Registration SET synchronization_state = 'deleted' WHERE mac_address = '{macAddress}'")
                msgServerDeleteAck = {
                    "operator": "server_delete_ack",
                    "status": 1,
                    "info": {
                        "room_id": roomID,
                        "mac_address": macAddress,
                        "time": utils.now()
                    }
                }
                client.publish(
                    serverTopics["server_delete_ack"], json.dumps(msgServerDeleteAck))
                print(f"DELETE {macAddress} at {utils.now()}")
            else:
                msgDelete = {
                    "operator": "gateway_delete",
                    "status": 1,
                    "info": {
                        "mac_address": item[2],
                        "time": utils.now()
                    }
                }
                client.publish(
                    thingTopics["gateway_delete"], json.dumps(msgDelete))
                print(f"GATEWAY DELETE {macAddress} at {utils.now()}")
            clock.sleep(10)


def registerProcess() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(thingTopics["register"])
    topic.append(thingTopics["register_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            print(msg)
            msg = json.loads(msg)
            print(msg["operator"])
            #print(msg["info"]["room_id"])
            if (msg["operator"] == "register"):
                print("registing")
                node_function = msg["info"]["node_function"]
                macAddress = msg["info"]["mac_address"]
                status = 1
                if (msg["info"]["mac_address"] in db.listAllValuesInColumn("Registration", "mac_address")):
                    nodeID = db.__do__(
                        f"SELECT node_id FROM Registration WHERE mac_address = '{macAddress}'")[0][0]
                    node_function = db.__do__(
                        f"SELECT node_function FROM Registration WHERE mac_address = '{macAddress}'")[0][0]
                    # db.__do__(
                    #     f"UPDATE Registration SET synchronization_state = 'synchronized' WHERE mac_address = '{macAddress}'")
                    status = 1
                else:
                    nodeID = utils.newNodeID(dbName)
                    node_function = msg["info"]["node_function"]
                    db.insertOneRecord("Registration", ["node_id", "node_function", "mac_address", "synchronization_state", "time"],
                                       [nodeID, node_function, macAddress, "unsynchronized", utils.now()])
                    status = 2
                msgServerAddAck = {
                    "operator": "register_ack",
                    "status": status,
                    "info": {
                        "room_id": roomID,
                        "node_id": nodeID,
                        "node_function": node_function,
                        "mac_address": macAddress,
                        "time": utils.now()
                    }
                }
                client.publish(
                    serverTopics["server_add_ack"], json.dumps(msgServerAddAck))
                print(f"ADD {macAddress} at {utils.now()}")


def addProcess() -> None:
    db = SqliteDAO(dbName)
    topic = []
    topic.append(serverTopics["server_add"])
    topic.append(serverTopics["server_add_ack"])
    client = Client(topic)
    client.connect(broker, port, keepalive)
    client.loop_start()
    while True:
        # wait to receive msg
        msg = client.msg_arrive()
        if (msg):
            print(msg)
            msg = json.loads(msg)
            print(msg["operator"])
            print(msg["info"]["room_id"])
            if (msg["operator"] == "server_add" and msg["info"]["room_id"]) == roomID:
                print("aaaaaaa")
                node_function = ""
                macAddress = msg["info"]["mac_address"]
                status = 0
                if (msg["info"]["mac_address"] in db.listAllValuesInColumn("Registration", "mac_address")):
                    nodeID = db.__do__(
                        f"SELECT node_id FROM Registration WHERE mac_address = '{macAddress}'")[0][0]
                    node_function = db.__do__(
                        f"SELECT node_function FROM Registration WHERE mac_address = '{macAddress}'")[0][0]
                    db.__do__(
                        f"UPDATE Registration SET synchronization_state = 'synchronized' WHERE mac_address = '{macAddress}'")
                    status = 1
                else:
                    nodeID = utils.newNodeID(dbName)
                    node_function = msg["info"]["node_function"]
                    db.insertOneRecord("Registration", ["node_id", "node_function", "mac_address", "synchronization_state", "time"],
                                       [nodeID, node_function, macAddress, "synchronized", utils.now()])
                    status = 2
                msgServerAddAck = {
                    "operator": "server_add_ack",
                    "status": status,
                    "info": {
                        "room_id": roomID,
                        "node_id": nodeID,
                        "node_function": node_function,
                        "mac_address": macAddress,
                        "time": utils.now()
                    }
                }
                client.publish(
                    serverTopics["server_add_ack"], json.dumps(msgServerAddAck))
                print(f"ADD {macAddress} at {utils.now()}")

# {"operator": "server_add", "info": {"room_id": "4", "node_function": "sensor", "mac_address": "test12", "time": 1701337245}}
# {"operator": "server_add", "status": 0, "info": {"room_id": 4, "node_function": "sensor", "mac_address": "DE:4F:22:58:96:59", "time": 1655396252}}
