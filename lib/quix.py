
import quixstreams as qx

connection = qx.kafka.configuration.ConnectionConfig(
    bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"],
    sasl_mechanism=os.environ["SASL_MECHANISM"],
    security_protocol=os.environ["SECURITY_PROTOCOL"],
    sasl_username=os.environ["SASL_USERNAME"],
    sasl_password=os.environ["SASL_PASSWORD"],
)

class MicroserviceApp(qx.Application):

class CustomWebsocketSource(qx.sources.base.BaseSource):
    def __init__(self, connection: ConnectionConfig, topic: str):
        super().__init__(connection, topic)

    def start(self):
        pass

    def stop(self):
        pass

    def on_message(self, message: Message):
        pass


    def on_error(self, error: Exception):
        pass


    def on_close(self):
        pass

    def on_open(self):
        pass

    def default_topic(self):
        return "default_topic"
