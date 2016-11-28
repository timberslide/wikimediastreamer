#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
import json

import socketIO_client
import gevent
import gevent.queue
from gevent import monkey
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()

from pytimberslide import Timberslide


TOKEN = ""
TOPIC = ""
TS = None
QUEUE = None


class WikiNamespace(socketIO_client.BaseNamespace):
    """Our socketio client listening for event from wikimedia"""
    count = 0
    def on_change(self, change):
        global QUEUE
        try:
            sys.stderr.write("Message: %s\n" % json.dumps(change))
            QUEUE.put_nowait(json.dumps(change))
            count += 1
            if count % 100:
                sys.stderr.write("Processed 100 messages\n")
        except Exception as err:
            sys.stderr.write("%s - Failed to send message to Timberslide - %s - %s\n" % (datetime.datetime.today(), change, err))
            return

    def on_connect(self):
        sys.stderr.write("%s - Subscribing to commons.wikimedia.org\n" % datetime.datetime.today())
        #self.emit('subscribe', '*')
        self.emit('subscribe', 'commons.wikimedia.org')


def socketio_thread(queue):
    """This thread starts the socketio client
    queue - a queue shared between our socketio worker and our timberslide worker
    """
    sys.stderr.write("%s - Starting socketio client\n" % datetime.datetime.today())
    while 1:
        try:
            sys.stderr.write("%s - Connecting to wikimedia\n" % datetime.datetime.today())
            socketIO = socketIO_client.SocketIO('https://stream.wikimedia.org')
            sys.stderr.write("%s - Setting up socketio namespace\n" % datetime.datetime.today())
            socketIO.define(WikiNamespace, '/rc')
            sys.stderr.write("%s - In wait\n" % datetime.datetime.today())
            socketIO.wait()
        except Exception as err:
            sys.stderr.write("%s - %s\n" % (datetime.datetime.today(), err))
            time.sleep(1)


def ts_thread(token, topic, queue):
    """This thread starts the timberslide client
    queue - a queue shared between our socketio worker and our timberslide worker
    """
    sys.stderr.write("%s - Starting Timberslide client\n" % datetime.datetime.today())
    while 1:
        sys.stderr.write("%s - Creating timberslide client\n" % datetime.datetime.today())
        TS = Timberslide(token)
        sys.stderr.write("%s - Calling timberslide connect()\n" % datetime.datetime.today())
        TS.connect()
        sys.stderr.write("%s - Attaching queue to timberslide stream\n" % datetime.datetime.today())
        TS.stream(topic, queue)


def main():
    global TOPIC
    global TS
    global QUEUE
    time.sleep(1) # take care of fast restarts

    # Configure ourselves
    TOKEN = os.getenv("APP_TOKEN")
    if TOKEN is None:
        sys.stderr.write("%s - Please set APP_TOKEN env variable\n" % datetime.datetime.today())
        sys.exit(1)
    TOPIC = os.getenv("APP_TOPIC")
    if TOPIC is None:
        sys.stderr.write("%s - Please set APP_TOPIC env variable\n" % datetime.datetime.today())
        sys.exit(1)

    QUEUE = gevent.queue.Queue()
    threads = [
        gevent.spawn(socketio_thread, QUEUE),
        gevent.spawn(ts_thread, TOKEN, TOPIC, QUEUE)
    ]
    gevent.joinall(threads)
    socketio_thread(QUEUE)

    sys.stderr.write("%s - All threads have exited\n" % datetime.datetime.today())


if __name__ == "__main__":
    main()
