#!/usr/bin/env python

import logging
import sys
# from qpid.messaging import *
import qpid.client
#, qmf.console
import Queue
from qpid.content import Content
# from qpid.message import Message
from qpid.harness import Skipped
from qpid.exceptions import VersionError
from qpid.spec08 import load
from qpid.connection import Connection
from qpid.util import connect
from qpid.session import Session
import qpid.delegate
import time
from qpid.datatypes import Message, RangedSet

logging.basicConfig(level=logging.DEBUG)

address = "amq.topic"

sock = connect("127.0.0.1", "5672")
conn = Connection(sock, username = "ssw", password = "ssw", vhost="/ssw",
        client_properties={"client_process":"aa"})
conn.start(timeout=10000)
session = conn.session("PYTHON_SESSION")

time.sleep(5)
session.queue_declare(queue="test-queue", exclusive=True, auto_delete=True)
session.exchange_declare("test", "direct")
session.exchange_bind(queue="test-queue", exchange="test", binding_key="key")

session.message_subscribe(queue="test-queue", destination="consumer_tag",
                accept_mode=session.accept_mode.none,
                acquire_mode=session.acquire_mode.pre_acquired)
session.message_flow(destination="consumer_tag", unit=session.credit_unit.message, value=0xFFFFFFFFL)
session.message_flow(destination="consumer_tag", unit=session.credit_unit.byte, value=0xFFFFFFFFL)
queue = session.incoming("consumer_tag")
delivery_properties = session.delivery_properties(routing_key="key")
sent = Message(delivery_properties, "Hello World!")
session.message_transfer(destination="test", message=sent)

input()
# conn.close()