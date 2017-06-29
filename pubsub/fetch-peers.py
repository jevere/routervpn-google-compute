#!/usr/bin/env python

from google.cloud import pubsub
from socket import gethostname
from sys import exit
from time import strftime,localtime

import errno
import logging
import argparse
import json

class Subscriber:
        MAX_MESSAGES=1024

        def __init__(self, logger, subs):
                self.subs = subs
                self.logger = logger
                self.name = subs.full_name.rsplit('/', 1)[-1]

                self.log().info('Subscriber: %s created' % (self.name))

        def log(self):
                return self.logger

        def pull(self):
                results = self.subs.pull(return_immediately=True, max_messages=self.MAX_MESSAGES)
                self.log().info('Sub:%s received %d messages' % (self.name, len(results)))

                latest = None
                ack_ids = []
                for ack_id, msg in results:
                        self.log().debug('* %s: %s' % (msg.message_id, msg.data))
                        ack_ids.append(ack_id)

                        next = json.loads(msg.data)
                        if not latest:
                                latest = next
                                continue
                        if int(latest['date']) < int(next['date']):
                                latest = next

                if len(ack_ids):
                        self.subs.acknowledge(ack_ids)

                        str = strftime('%Y-%m-%d %H:%M:%S', localtime(int(latest['date']) / 1000))
                        self.log().info('Latest pulled: %s ip: %s' % (str, latest['ip']))

                return latest

        def getname(self):
                return str(self.name)

class MappingInfo:
        def __init__(self, hostname):
                self.hostname = hostname

                logging.basicConfig(level=logging.INFO)
                logger = logging.getLogger()

                self.logger = logger
                self.hostname = hostname
                self.client = pubsub.Client()
                self.subscribers = []
                self.state_name = '%s.state' % (self.hostname)

        def log(self):
                return self.logger

        def _pull_subs(self):

                subscribers = []
                for topic in self.client.list_topics():
                        topic_name = topic.full_name.rsplit('/', 1)[-1]
                        if topic_name != hostname:
                                self.log().info('Topic: %s found' % (topic_name))

                                subs = topic.subscription(self.hostname)
                                if not subs.exists():
                                        self.log().warning('Topic: %s does not have subscription %s' %
                                                      (topic_name, self.hostname))
                                        continue
                                subscribers.append(Subscriber(self.logger, subs))

                self.subscribers = subscribers

        def load(self):
                self.log().info('Loading previous state...')
                try:
                        with open(self.state_name, 'r') as f:
                                self.mappings = json.loads(f.read())
                        self.log().info('loaded mappings: %s' % self.mappings)

                except:
                        self.log().info('Previuos state %s could not be loaded' % self.state_name)
                        self.mappings = json.loads('{ "subs": { } }')


        def save(self):
                self.log().info('Saving new state...')
                try:
                        with open(self.state_name, 'w') as f:
                                json.dump(self.mappings, f, sort_keys=True, indent=4)
                except Exception as e:
                        self.log().error('Can\'t save state %s, mappings will be lost: %s' % (self.state_name, e))

        def pull(self):

                self.log().info('Pulling information...');
                self._pull_subs()

                # Drain the queue (pull messages until there are not new)
                while True:
                        retry = False
                        for subscriber in self.subscribers:
                                latest = subscriber.pull()
                                if latest:
                                        name = subscriber.getname()
                                        if name in self.mappings["subs"]:
                                                current = self.mappings["subs"][name]
                                                if int(current['date']) > int(latest['date']):
                                                        self.log().info('Current is newer than latest: %s' % (name))
                                                        latest = current

                                        self.mappings["subs"][name] = latest
                                        str = strftime('%Y-%m-%d %H:%M:%S', localtime(int(latest['date']) / 1000))
                                        self.log().info('Updating %s: %s ip: %s' % (name, str, latest['ip']))
                                        retry = True
                                                
                                
                        if not retry:
                                break
                

if __name__ ==  '__main__':

        # Parse Arguments
        parser = argparse.ArgumentParser(description='Fetch peers')
        parser.add_argument('--hostname', help='Overwrite hostname')
        parser.add_argument('--noack', help='Don\'t ack messages', action="store_true")

        hostname = gethostname()
        args = parser.parse_args()
        if args.hostname:
                hostname = args.hostname

        mappings = MappingInfo(hostname)

        mappings.load()
        mappings.pull()
        mappings.save()
