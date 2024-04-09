import paho.mqtt.client as mqtt
from Libraly.logs import Log

# Inheritance class
class Client(mqtt.Client):
    def __init__(self, topic):
        super().__init__()
        self.__check = False
        self.__logger = Log(__name__)
        self.__topic = topic

    def on_connect(self, client, userdata, flags, rc):
        """Called when the broker responds to our connection request"""
        # The connection result
        if rc == 0:
            self.__logger.info("Connected to broker")
            for topic in self.__topic:
                self.subscribe(topic)
    
    def on_connect_fail(self, client, userdata):
        """Called when the client failed to connect to the broker"""
        self.__logger.info("Unconnected to broker")

    def on_disconnect(self, client, userdata, rc):
        if rc == 0:
            self.__logger.info("Disconnected to broker")

    def on_message(self, client, userdata, message):
        """Called when a message has been received on a topic that the client subscribes to"""
        self.__logger.info("Received message from broker")
        self.__check = True
        self.__msg = message.payload.decode("utf-8")

    def on_publish(self, client, userdata, mid):
        '''Called when publish() function has been used'''
        self.__logger.info("Published successfully")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        """Publish a message on a topic"""
        self.__logger.info(f"Subscribed to {self.__topic} successfully")

    def msg_arrive(self):
        if self.__check == True:
            self.__check = False
            return self.__msg
        return None

if __name__=="__main__":
    pass