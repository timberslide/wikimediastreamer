"""
pytimberslide

This is a nice wrapper around Timberslide's gRPC API
"""
import grpc

import timberslide_pb2


HOST = "gw.timberslide.com:443"


class MissingTokenException(Exception):
    pass


class Timberslide(object):
    def __init__(self, token, host=None):
        self.host = host
        if self.host is None:
            self.host = HOST
        if token is None:
            raise MissingTokenException("Please supply a token")
        self.metadata = [(b'ts-access-token', token)]
        self.creds = grpc.ssl_channel_credentials(
            root_certificates=None, private_key=None, certificate_chain=None)

    def connect(self):
        """Establish a connection to Timberslide"""
        self.channel = grpc.secure_channel(self.host, self.creds)

    def close(self):
        """Closes the connection to Timberslide"""
        pass

    def get_topic(self, topic):
        """Returns a generator that gives the messages in the topic"""
        stub = timberslide_pb2.StreamerStub(self.channel)
        stream = stub.GetStream(timberslide_pb2.Topic(Name=topic),
                                metadata=self.metadata)
        for message in stream:
            yield message.Message

    def send(self, topic, message):
        stub = timberslide_pb2.IngestStub(self.channel)
        result = stub.StreamEvents([timberslide_pb2.Event(Topic=topic, Message=message)],
                          metadata=self.metadata)
        result.result()
