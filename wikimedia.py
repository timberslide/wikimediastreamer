#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
import json

import socketIO_client

from pytimberslide import Timberslide


TOKEN = ""
TOPIC = ""
TS = None


class WikiNamespace(socketIO_client.BaseNamespace):
    count = 0
    def on_change(self, change):
        TS.send(TOPIC, json.dumps(change))
        self.count += 1
        if datetime.datetime.now().timetuple().tm_sec == 0:
            sys.stderr.write("%s - Processed %d messages\n" % (datetime.datetime.today(), self.count))

    def on_connect(self):
        sys.stderr.write("%s - Subscribing to *\n" % datetime.datetime.today())
        #self.emit('subscribe', '*')
        self.emit('subscribe', 'commons.wikimedia.org')

def main():
    global TOPIC
    global TS
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

    # Set up our Timberslide client
    sys.stderr.write("%s - Connecting to timberslide\n" % datetime.datetime.today())
    TS = Timberslide(TOKEN)
    TS.connect()

    # Start up the feed
    sys.stderr.write("%s - Starting\n" % datetime.datetime.today())
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

if __name__ == "__main__":
    main()
