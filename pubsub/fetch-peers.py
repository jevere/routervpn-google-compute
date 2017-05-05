#!/usr/bin/env python

from google.cloud import pubsub
from socket import gethostname
from sys import exit
from time import strftime,localtime

import errno
import logging
import argparse
import json


class AddressSubscriber:
        MAX_MESSAGES = 128

        def __init__(self, hostname):

                logging.basicConfig(level=logging.DEBUG)
                logger = logging.getLogger(__name__)

                self.logger = logger
                self.hostname = hostname
                self.client = pubsub.Client()

                logger.info('Fetching for {}'.format(self.hostname))

        def load_topics(self):

                topics = []
                topics_name = []
                for topic in self.client.list_topics():
                        name = topic.full_name.rsplit('/', 1)[-1]
                        if name != hostname:
                                topics.append(topic)
                                topics_name.append(name)

                self.topics = topics
                self.logger.info('List of topics: {}'.format(topics_name))

        def fetch(self):

                for topic in self.topics:
                        self._fetch_topic(topic)

        def _fetch_topic(self, topic):

                subs = topic.subscription(self.hostname)
                topic_name = topic.full_name.rsplit('/', 1)[-1]

                if not subs.exists():
                        self.logger.error('Sub:{} does not exists for topic {}'.format(self.hostname, topic.full_name))
                        return

                results = subs.pull(return_immediately=True, max_messages=self.MAX_MESSAGES)
                self.logger.info('Sub:{} received {} messages.'.format(subs.full_name, len(results)))

                state_name = '{}.{}.state'.format(self.hostname, topic_name)
                cur = None

                try:
                        with open(state_name, 'r') as infile:
                                cur = json.load(infile)
                except IOError:
                        self.logger.error('{} can\'t be opened, discarding old content'.format(state_name))

                updated = False

                ack_ids = []
                for ack_id, message in results:
                        self.logger.debug('* {}: {}'.format(message.message_id, message.data))
                        ack_ids.append(ack_id)

                        next = json.loads(message.data)
                        if not cur:
                                cur = next;
                                updated = True
                                continue

                        if int(cur['date']) < int(next['date']):
                                cur = next
                                updated = True

                if len(ack_ids):
                        subs.acknowledge(ack_ids)

                        str = strftime('%Y-%m-%d %H:%M:%S', localtime(int(cur['date']) / 1000))
                        self.logger.info('Latest update: {} ip: {}'.format(str, cur['ip']))

                        if updated:
                                self.logger.info('Updating {}'.format(state_name))
                                with open('{}.{}.state'.format(self.hostname, topic_name), 'w') as outfile:
                                        json.dump(cur, outfile)

if __name__ ==  '__main__':

        # Parse Arguments
        parser = argparse.ArgumentParser(description='Fetch peers')
        parser.add_argument('--hostname', help='Overwrite hostname')
        parser.add_argument('--noack', help='Don\'t ack messages', action="store_true")

        hostname = gethostname()
        args = parser.parse_args()
        if args.hostname:
                hostname = args.hostname

        Addr = AddressSubscriber(hostname)
        Addr.load_topics()
        Addr.fetch()
