#!/usr/bin/python2.7

import ConfigParser
from datetime import datetime, timedelta
import email.utils
import logging
from multiprocessing import BoundedSemaphore, Process, Queue
from threading import Thread

from cache import Cache
from nntp import Worker

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

class Indexer:
  def __init__(self, config_file='defaults.cfg'):
    self.config = ConfigParser.SafeConfigParser()
    self.config.readfp(open(config_file))

    self.action_queue = Queue()
    self.actions = BoundedSemaphore(20)
    self.starter = Thread(target=self.action_starter, name='starter',
                          args=(self.action_queue,))
    self.starter.daemon = True
    self.starter.start()

    self.nntp_c = Cache(self.config, True)
    max_connections = self.config.getint('indexer', 'max_connections')
    self.nntp_semaphore = BoundedSemaphore(max_connections)

  @property
  def nntp_connection(self):
    worker = Worker(
        self.nntp_semaphore,
        self.config.get('indexer', 'host'),
        self.config.get('indexer', 'port'),
        self.config.get('indexer', 'username'),
        self.config.get('indexer', 'password'),
        True)
    #worker.set_debuglevel(1)
    return worker

  @property
  def cache_connection(self):
    return Cache(self.config)

  def update_groups(self):
    self.build_group_list()

  def update_watched(self):
    with self.cache_connection as cache:
      for watched in cache.get_watched():
        self.update_group(watched)

  def update_group(self, group):
    last = 0
    last_read = self.nntp_c.get_last_read(group)
    span = self.config.getint('indexer', 'xover_range')
    with self.nntp_connection as nntp:
      resp, count, first, last, group_name = nntp.group(group)
      last_read = max(int(first), last_read)
      last = int(last)

    LOG.debug('update_group: %s %d %d', group, last, last_read)

    if last > last_read:
      for start in xrange(last_read, last, span):
        end = min(last, start + span)
        self.do('fetch', group, start, end)
        #self.start_action(self.fetch_group_articles, (group, start, end),
        #    'get_group_articles %s %s %s' % (group, start, end))

  def do(self, *args):
    self.action_queue.put(args)

  # Actions, may be in a process or thread! Beware DB locking!
  def action_starter(self, queue):
    while True:
      proc = None
      args = list(queue.get())
      action = args.pop(0)
      LOG.debug(args)
      with self.actions:
        if action == 'fetch':
          proc = Thread(target=self.fetch_group_articles, args=args)
          proc.daemon = True
        else:
          LOG.error('Invalid action provided: %s', action)
      if proc:
        proc.start() # possible deadlock if in the with statement

  def build_group_list(self, all=False):
    try:
      with self.actions:
        with self.nntp_connection as nntp:
          resp, group_list = nntp.list()

        with self.cache_connection as cache:
          # count, first, last, name
          cache.add_groups([ (g[0],) for g in group_list ])
    except KeyboardInterrupt:
      #LOG.critical('KB INTERRUPT')
      return

  def fetch_group_articles(self, group, start, end):
    def for_cache(article):
      t = email.utils.parsedate_tz(article[3])
      posted = datetime(*t[:6]) - timedelta(seconds=t[-1])
      return posted, article[1], article[4], group, article[0]

    try:
      with self.actions:
        with self.nntp_connection as nntp:
          LOG.info("fetching %s: %s - %s ...", group, start, end)
          group_resp = nntp.group(group)
          resp, articles = nntp.xover(str(start), str(end))
          LOG.debug('articles recieved')

        with self.cache_connection as cache:
          LOG.info("storing %s: %s - %s ...", group, start, end)
          # a_no, subject, poster, when, a_id, refs, sz, li
          cache.add_articles(map(for_cache, articles))
    except KeyboardInterrupt:
      #LOG.critical('KB INTERRUPT')
      return

if __name__ == '__main__':
  app = Indexer('defaults.cfg')
  app.build_group_list()
  with app.cache_connection as cache:
    cache.set_watched('alt.binaries.tv', True)
    cache.set_watched('alt.binaries.hdtv', True)
  app.update_watched()
