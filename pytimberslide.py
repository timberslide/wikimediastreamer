"""
pytimberslide

This is a nice wrapper around Timberslide's gRPC API
"""
import sys
import datetime

import grpc
import gevent.queue
from gevent import monkey
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()

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

        print "Timberslide config:", self.host, self.metadata

    def connect(self):
        """Establish a connection to Timberslide"""
        self.channel = grpc.secure_channel(self.host, self.creds)
        print "Timberslide connected"

    def close(self):
        """Closes the connection to Timberslide"""
        print "Timberslide close() called"

    def get_topic(self, topic):
        """Returns a generator that gives the messages in the topic"""
        stub = timberslide_pb2.StreamerStub(self.channel)
        stream = stub.GetStream(timberslide_pb2.Topic(Name=topic),
                                metadata=self.metadata)
        for message in stream:
            yield message.Message

    #def send(self, topic, message):
    #    """Send a single event into Timberslide"""
    #    stub = timberslide_pb2.IngestStub(self.channel)
    #    result = stub.StreamEvents([timberslide_pb2.Event(Topic=topic, Message=message)],
    #                      metadata=self.metadata)
    #    result.result()

    def make_message(self, topic, message):
        sys.stderr.write("Creating message: %s\n", message)
        return timberslide_pb2.Event(Topic=topic, Message=message)

    def message_generator(self, topic, messages):
        """Send a single event into Timberslide"""
        sys.stderr.write("%s - creating message generator %s %s\n" % (datetime.datetime.today(), topic, messages))
        while 1:
            message = messages.get_nowait()
            if not message:
                gevent.sleep(0)
                continue
            yield self.make_message(topic, message)

    def stream(self, topic, messages):
        """Takes an iterator and streams the events into Timberslide"""
        sys.stderr.write("%s - timberslide stream got topic and queue %s %s\n" % (datetime.datetime.today(), topic, messages))
        stub = timberslide_pb2.IngestStub(self.channel)
        sys.stderr.write("%s - timberslide calling StreamEvents\n" % (datetime.datetime.today()))
        result = stub.StreamEvents(self.message_generator(topic, messages), metadata=self.metadata)
        sys.stderr.write("%s - timberslide calling result.result()\n" % (datetime.datetime.today()))
        result.result()
