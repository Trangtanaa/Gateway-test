import os
import calendar
import datetime
import sqlite3
from Libraly.dao import SqliteDAO

# suggest use .env file
dbName = "./data/data.db"
#broker = "test.mosquitto.org"
broker = "broker.hivemq.com"
broker_sv = "broker.hivemq.com"
roomID = 1
# broker = "broker.emqx.io"
port = 1883
keepalive = 60
thingTopics = {"register": f"farm/register",
                "register_ack": f"farm/register",
                "keep_alive": f"farm/{roomID}/alive",
                "keep_alive_ack": f"farm/{roomID}/alive",
                "gateway_delete":f"farm/{roomID}/sync_node",
                "gateway_delete_ack":f"farm/{roomID}/sync_node",
                "gateway_add":f"farm/{roomID}/sync_node",
                "gateway_add_ack":f"farm/{roomID}/sync_node",
                "sensor_data": f"farm/{roomID}/sensor",
                "energy_data": f"farm/{roomID}/sensor",
                "actuator_data": f"farm/{roomID}/actuator",
                "setPoint": f"farm/{roomID}/actuator",
                "setpoint_ack": f"farm/{roomID}/actuator",
                "commandline": f"farm/{roomID}/commandline" }

serverTopics = {"data_request": "farm/monitor/sensor",
                  "data_response": "farm/monitor/sensor",
                  "energy_data" : "farm/monitor/sensor",
                  "actuator_data": "farm/monitor/process",
                  "send_setpoint": "farm/control",
                  "send_setpoint_ack": "farm/control", 
                  "server_delete": "farm/sync_node",
                  "server_delete_ack": "farm/sync_node_ack",
                  "server_add": "farm/sync_node",
                  "server_add_ack": "farm/sync_node_ack",}


def creatDatabase():
    db = SqliteDAO(dbName)
    db.createTable("Registration", """  node_id INTEGER PRIMARY KEY, 
                                        node_function TEXT(50),
                                        mac_address TEXT(20),
                                        synchronization_state TEXT(50), 
                                        time INTEGER""")
    db.createTable("SensorMonitor", """id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        node_id INTEGER,
                                        co2 INTEGER,
                                        temp REAL,
                                        hum REAL,
                                        light REAL,
                                        sound REAL,
                                        dust REAL,
                                        red INTERGER, 
                                        green INTERGER,
                                        blue INTERGER,
                                        motion INTEGER,
                                        time INTEGER,
                                        FOREIGN KEY (node_id) REFERENCES Registration (node_id) ON DELETE CASCADE ON UPDATE CASCADE""")
    db.createTable("EnergyMonitor", """id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        node_id INTEGER,
                                        voltage REAL,
                                        current REAL,
                                        active_power REAL,
                                        power_factor REAL,
                                        frequency REAL,
                                        active_energy REAL,
                                        time INTEGER""")
    db.createTable("ActuatorMonitor", """id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        node_id INTEGER,
                                        speed INTEGER,
                                        state INTEGER,
                                        time INTEGER,
                                        FOREIGN KEY (node_id) REFERENCES Registration(node_id) ON DELETE CASCADE ON UPDATE CASCADE""")
    db.createTable("SetPointControl", """id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        node_id INTEGER,
                                        option TEXT(10),
                                        aim TEXT(10),
                                        value REAL,
                                        time INTEGER,
                                        FOREIGN KEY (node_id) REFERENCES Registration(node_id) ON DELETE CASCADE ON UPDATE CASCADE""")

def creatDatabaseSchedule():
    db = SqliteDAO(dbName)
    items = db.listAllValues("Registration")
    date = datetime.datetime.utcfromtimestamp(now())
    newFileName = f"./data/data_{date.month}_{date.year}.db"
    os.rename("./data/data.db", newFileName)
    creatDatabase()
    db = SqliteDAO(dbName)
    for item in items:
        db.insertOneRecord(
            "Registration", ["id", "mac_address"], item)

def infoJsonToColValues(json, keyList) -> tuple:
    ret = []
    for key in json:
        if key == "info":
            for i in keyList:
                if i in (json[key]).keys():
                    ret.append(json[key][i])
        if key == "status" and "status" in keyList:
            ret.append(json["status"])
    # print(ret)
    if (None in ret):
        return []
    else:
        return tuple(ret)

"""
*brief: this function is to get the needed number of latest data in database and
        compute out the average value of those
"""

def getAverageData(dbName, id,  tableName, colName, numberData=10) -> dict:
    ret = {}
    __conn = sqlite3.connect(dbName)
    __cur = __conn.cursor()
    for col in colName:
        res = __cur.execute(
                f"SELECT {col} FROM {tableName} WHERE node_id = ? ORDER BY id DESC LIMIT ?", (id, numberData))
        res_list = res.fetchall()
        if res_list:
            average = round(sum(data[0] for data in res_list)/len(res_list), 2)
            if(average<0):
                average = -1
            ret[col] = average
        else:
            ret[col] = -1
    __cur.close()
    __conn.close()
    # print(ret)
    return ret

def now() -> int:
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())+7*3600

def dataFilter(json) -> bool:
    if(None in json["info"].values()):
        return False
    else:
        return True

def newNodeID(dbName) -> int:
    retNodeID = 1
    db = SqliteDAO(dbName)
    oldNodeIDs = db.listAllValuesInColumn("Registration", "node_id")
    while(retNodeID in oldNodeIDs):
        retNodeID+=1
    return retNodeID
