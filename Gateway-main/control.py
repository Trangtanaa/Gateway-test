from Libraly.mqtt import Client
from Libraly.dao import SqliteDAO
import json
import time as clock
import utils

dbName = utils.dbName
broker = utils.broker
broker_sv=utils.broker_sv
port = utils.port
keepalive = utils.keepalive
thingTopics = utils.thingTopics
serverTopics = utils.serverTopics


def sendSetPointProcess() -> None:
    db = SqliteDAO(dbName)
    svtopic = []
    thtopic = []
    etopic = []
    state=1
    trial=1
    ac_current=3
    svtopic.append(serverTopics["send_setpoint"])
    svtopic.append(serverTopics["send_setpoint_ack"])
    thtopic.append(thingTopics["send_setpoint"])
    thtopic.append(thingTopics["send_setpoint_ack"])
    etopic.append(thingTopics["commandline"])
    #topic.append(thingTopics["send_setpoint"])
    #topic.append(thingTopics["send_setpoint_ack"])
    svclient = Client(svtopic)
    thclient = Client(thtopic)
    eclient=Client(etopic)
    svclient.connect(broker_sv, port, keepalive)
    thclient.connect(broker, port, keepalive)
    svclient.loop_start()
    #thclient.loop_start()
    while True:
    #STATE 1-------------------------------RECEIVE CONTROL DATA FROM SERVER--------------------------------------
        if (state==1):
            msg = svclient.msg_arrive()
            if (msg):
                msg = json.loads(msg)
                # print(msg)
                if msg["operator"] == "ac_control":
                    if msg["info"]["room_id"]==roomID:
                        allnodeID=db.__do__("SELECT node_id FROM Registration WHERE node_function = 'air_conditioner'")
                        if msg["info"]["node_id"] in allnodeID:
                            node_id=msg["info"]["node_id"]
                            power= msg["info"]["power"]
                            temp = msg["info"]["temp"]
                            state=2
    #STATE 2-------------------------------RECEIVE EDATA FROM ENERGY SENSOR---------------------------------------
        if(state==2):
            edata_request= {
                "operator":"edata_request",
                "node_id":8
            }
            #send data request to energy sensor
            eclient.publish(thingTopics["commandline"], json.dumps(edata_request))                      
            msg=eclient.msg_arrive()
            if (msg):
                msg= json.loads(msg)
                if(msg["operator"]=="edata_request_ack"):
                    if(msg["info"]["node_id"]==20):
                        pre_current = msg["info"]["current"]
                        ac_control = {
                            "operator":"ac_control",
                            "info":{
                                "node_id": 8,
                                "power": power,
                                "temp": temp,
                                "time": utils.now
                            }
                        }
                        #send control data to ac remote
                        thclient.publish(thingTopics["send_setpoint"], json.dumps(ac_control))
                        state=3
                    else:
                        ac_control_ack = {
                            "operator":"ac_control_ack",
                            "status":0,
                            "info":{
                                "room_id": roomID,
                                "node_id": node_id,
                                "error": "edata_error",
                                "time": utils.now
                            }
                        }
                        svclient.publish(serverTopics["send_setpoint"],json.dumps(ac_control_ack))
                        state=1
    #STATE 3--------------------------------RECEIVE ACK_DATA FROM AC_REMOTE--------------------------------------
        if(state==3):
            msg=thclient.msg_arrive()
            if(msg):
                msg=json.loads(msg)
                if msg["info"]["node_id"]==node_id:
                    if msg[status]==1:
                        edata_request= {
                            "operator":"edata_request",
                            "node_id":8
                        }
                        #send data request to energy sensor
                        eclient.publish(thingTopics["commandline"], json.dumps(edata_request)) 
                        state=4
                    else:
                        ac_control_ack = {
                            "operator":"ac_control_ack",
                            "status":0,
                            "info":{
                                "room_id": roomID,
                                "node_id": node_id,
                                "error": "ac_remote_error",
                                "time": utils.now
                            }
                        }
                        #send error to server
                        svclient.publish(serverTopics["send_setpoint"],json.dumps(ac_control_ack))
                        state=1                                               

    #STATE 4---------------------------------RECEIVE EDATA FROM ENERGY SENSOR-------------------------------------
        if(state==4):                        
            msg=eclient.msg_arrive()
            if (msg):
                msg= json.loads(msg)
                if(msg["operator"]=="edata_request_ack"):
                    if(msg["info"]["node_id"]==20):
                        flw_current = msg["info"]["current"]
                        if flw_current > (pre_current+ac_current):
                            ac_control_ack = {
                                "operator":"ac_control_ack",
                                "status":1,
                                "info":{
                                    "room_id": roomID,
                                    "node_id": node_id,
                                    "error": "Null",
                                    "time": utils.now
                                }
                            }
                            #send success noti to server
                            svclient.publish(serverTopics["send_setpoint"],json.dumps(ac_control_ack))
                            state=1 
                        else:  
                            ac_control_ack = {
                                "operator":"ac_control_ack",
                                "status":0,
                                "info":{
                                    "room_id": roomID,
                                    "node_id": node_id,
                                    "error": f"failed, trial : {trial}",
                                    "time": utils.now
                                }
                            }
                            #send trial noti to server
                            svclient.publish(serverTopics["send_setpoint"],json.dumps(ac_control_ack))
                            trial=trial+1
                            #if not max trials
                            if trial<6:
                                state=2
                            #if max trial
                            else:
                                state=1
                                trial=1
                    else:
                        ac_control_ack = {
                            "operator":"ac_control_ack",
                            "status":0,
                            "info":{
                                "room_id": roomID,
                                "node_id": node_id,
                                "error": "edata_error",
                                "time": utils.now
                            }
                        }
                        #send error to server
                        svclient.publish(serverTopics["send_setpoint"],json.dumps(ac_control_ack))
                        state=1
    svclient.loop_stop()
