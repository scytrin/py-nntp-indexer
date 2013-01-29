#!/usr/bin/python2.7

import ConfigParser
from datetime import datetime, timedelta
import logging
from Queue import Queue
import threading
import time

from cache import Cache
from nntp import Worker

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

class Indexer:
  def __init__(self, config_file='defaults.cfg'):
    self.config = ConfigParser.SafeConfigParser()
    self.config.readfp(open(config_file))

    self.cache_connection._initialize()

    max_nntp = self.config.getint('indexer', 'max_connections')
    self.nntp_semaphore = threading.BoundedSemaphore(max_nntp)

    max_task = 5
    self.task_queue = Queue()
    self.task_runners = []
    for i in xrange(max_task):
      runner = threading.Thread(target=self.task_runner)
      runner.daemon = True
      self.task_runners.append(runner)
      runner.start()


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
    return Cache(self.config.get('indexer', 'cache_file'))

  def update_groups(self):
    self.add_task('update_groups', False)

  def update_watched(self):
    with self.cache_connection as cache:
      LOG.debug(cache)
      for watched in cache.get_watched():
        LOG.debug(watched)
        self.get_last(watched, 1000)

  def update_group(self, group):
    with self.cache_connection as cache:
      last_read = cache.get_last_read(group)
    last = 0
    span = self.config.getint('indexer', 'xover_range')
    with self.nntp_connection as nntp:
      resp, count, first, last, group_name = nntp.group(group)
      last_read = max(int(first), last_read)
      last = int(last)
    LOG.debug('update_group: %s %d %d', group, last, last_read)
    if last > last_read:
      for start in xrange(last_read, last, span):
        end = min(last, start + span - 1)
        self.add_task('fetch', group, start, end)

  def get_last(self, group, number):
    LOG.debug((group, number))
    span = self.config.getint('indexer', 'xover_range')
    with self.nntp_connection as nntp:
      resp, count, first, last, group_name = nntp.group(group)
    last = int(last)
    start = max(int(first), last - number)
    if start < last:
      for point in xrange(start, last, span):
        end = min(last, point + span - 1)
        self.add_task('fetch', group, point, end)


  def add_task(self, *args):
    LOG.debug(args)
    self.task_queue.put(args)

  def task_runner(self):
    try:
      while True:
        task = self.task_queue.get()
        LOG.debug(task)
        if task[0] == 'fetch':
          self._fetch_group_articles(*task[1:])
        elif task[0] == 'update_groups':
          self._build_group_list(*task[1:])
        else:
          LOG.error('Invalid action provided: %s', task)
        self.task_queue.task_done()
    except KeyboardInterrupt as err:
      LOG.error(err)
      return

  def _build_group_list(self, all=False):
    with self.nntp_connection as nntp:
      resp, groups = nntp.list()
    with self.cache_connection as cache:
      cache.add_groups(groups)

  def _fetch_group_articles(self, group, start, end):
    LOG.debug((group, start, end))
    with self.nntp_connection as nntp:
      nntp.group(group)
      resp, articles = nntp.xover(str(start), str(end))
    LOG.debug(len(articles))
    if articles:
      with self.cache_connection as cache:
        cache.add_articles(group, articles)

if __name__ == '__main__':
  app = Indexer('defaults.cfg')
  app.update_groups()
  with app.cache_connection as cache:
    cache.set_watched('alt.binaries.tv', True)
    cache.set_watched('alt.binaries.hdtv', True)
  app.update_watched()
  while not app.task_queue.empty():
    time.sleep(1)
