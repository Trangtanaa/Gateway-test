import os
import calendar
import datetime
from Libraly.logs import Log
import multiprocessing
import sync
import dataSensor 
import dataActuator
import control
import utils

def main(): 
    try:
        # create logger file if not exist
        logger = Log(__name__)
        logger.info("-------------Restart----------------")
        # create database in folder "data"
        if ("data" in os.listdir()) == False:
            os.mkdir("data")
        utils.creatDatabase()

        # start multiprocess
        process_list = []
        process_list.append(multiprocessing.Process(target=sync.registerProcess))
        process_list.append(multiprocessing.Process(target=sync.keepAliveProcess))
        process_list.append(multiprocessing.Process(target=sync.keepAliveAckProcess))
        process_list.append(multiprocessing.Process(target=sync.serverDeleteProcess))
        process_list.append(multiprocessing.Process(target=sync.gatewayDeleteProcess))
        process_list.append(multiprocessing.Process(target=sync.addProcess))
        process_list.append(multiprocessing.Process(target=dataSensor.store_sData))
        #process_list.append(multiprocessing.Process(target=dataSensor.store_eData))
        process_list.append(multiprocessing.Process(target=dataSensor.publish_sData))
        #process_list.append(multiprocessing.Process(target=dataSensor.publish_eData))
        # process_list.append(multiprocessing.Process(target=dataActuator.storeData))
        # process_list.append(multiprocessing.Process(target=dataActuator.pulishData))
        process_list.append(multiprocessing.Process(target=control.sendSetPointProcess))
        for i in process_list:
            i.start()

        # Read the time to use for automatically creating a new database every month
        timestamp = calendar.timegm(datetime.datetime.utcnow().utctimetuple())+7*3600
        date = datetime.datetime.utcfromtimestamp(timestamp)
        preMonth = date.month
        while (True):
            timestamp = calendar.timegm(datetime.datetime.utcnow().utctimetuple())+7*3600
            date = datetime.datetime.utcfromtimestamp(timestamp)
            newMonth = date.month 
            # Check to see if the new month has passed
            if (newMonth != preMonth):
                # Stop all processes to avoid file renaming conflicts when other tasks are pointing to related files
                for i in process_list:
                    i.terminate()
                for i in process_list:
                    i.join()
                # creat new database
                utils.creatDatabaseSchedule()
                logger.info("-------------New DB----------------")
                # start all process again
                process_list = []
                process_list.append(multiprocessing.Process(target=sync.registerProcess))
                process_list.append(multiprocessing.Process(target=sync.keepAliveProcess))
                process_list.append(multiprocessing.Process(target=sync.keepAliveAckProcess))
                process_list.append(multiprocessing.Process(target=sync.serverDeleteProcess))
                process_list.append(multiprocessing.Process(target=sync.gatewayDeleteProcess))
                process_list.append(multiprocessing.Process(target=sync.addProcess))
                #process_list.append(multiprocessing.Process(target=dataSensor.store_sData))
                process_list.append(multiprocessing.Process(target=dataSensor.store_eData))
                #process_list.append(multiprocessing.Process(target=dataSensor.publish_sData))
                process_list.append(multiprocessing.Process(target=dataSensor.publish_eData))
                # process_list.append(multiprocessing.Process(target=dataActuator.storeData))
                # process_list.append(multiprocessing.Process(target=dataActuator.pulishData))
                process_list.append(multiprocessing.Process(target=control.sendSetPointProcess))
                for i in process_list:
                    i.start()
                preMonth = newMonth

    except KeyboardInterrupt as error:
        logger.error("Interrupt from keyboard")
        print(str(error))
    except Exception as error:
        print("_______________________________________________________")
        logger.exception(error)
        print(str(error))
    finally:
        logger.info("-------------End----------------")

    print("FINISH")


if __name__ == "__main__":
    main()
