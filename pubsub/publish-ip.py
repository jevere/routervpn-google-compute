#!/usr/bin/env python

from google.cloud import pubsub
from socket import gethostname
from sys import exit
from urllib2 import urlopen

import syslog

hostname = gethostname()
client = pubsub.Client()

syslog.syslog('{} Starting publisher'.format(hostname))

topic = client.topic(hostname)
if not topic.exists():
	syslog.syslog(syslog.LOG_ERR, 'topic {} does not exists'.format(hostname))
	exit(1)

result = urlopen("https://us-central1-routervpn-166019.cloudfunctions.net/IpEcho").read()

message_id = topic.publish(result)
print('Topic: {} Message {} published: {}'.format(topic.full_name, message_id, result))
