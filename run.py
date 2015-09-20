#!/usr/bin/python2

import collections
import interval
import itertools
import logging
from pprint import pformat
import Queue
import re
import sys
import threading
import time
import yaml

import memory_profiler

import nntp
import store


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def _decode_if_str(string):
  if not isinstance(string, str):
    return string
  try:
    return unicode(string, 'ascii')
  except UnicodeDecodeError:
    for encoding in ('latin-1', 'cp037'):
      try:
        u_str = unicode(string, encoding)
        LOG.debug('Encoding success: %s %s', encoding, repr(u_str))
        return u_str
      except UnicodeDecodeError:
        LOG.warn('Encoding failure: %s %s', encoding, repr(string))
    return unicode(string, 'ascii', 'replace')


def _compress_ints(ints, initial=None,  func=tuple):
  last = None
  intervalset = interval.IntervalSet()
  for curr in sorted(set(ints)):
    if initial is None:
      last = curr
      initial = curr
    if abs(last - curr) > 1:
      intervalset.add(interval.Interval.between(initial, last))
      initial = curr
    last = curr
  intervalset.add(interval.Interval.between(initial, last))
  return intervalset


def ArticleQueueProcessor(exit_event, queue):
  while not exit_event.is_set():
    try:
      group_name, nntp_article = queue.get_nowait()
      #LOG.debug(nntp_article)
      article = store.Article.addFromNNTP(nntp_article)
      article.addGroupIndex(group_name, nntp_article[0])
      for match in article.getSegmentData():
        article.addSegment(match)
        break
      queue.task_done()
    except Queue.Empty as err:
      time.sleep(3)


def QueueArticlesFromServer(exit_event, queue, group_name, server):
  LOG.info('Queueing articles from %s on %s', group_name, server)
  q = store.GroupIndex.select(store.GroupIndex.number)
  q = q.where(store.GroupIndex.name == group_name)
  article_nums = _compress_ints(i[0] for i in q.tuples())
  LOG.info(article_nums)

  with server as nntp:
    group_resp, count, g_first, g_last, name = nntp.group(group_name)
    LOG.info((group_name, g_first, g_last, len(article_nums)))
    all_articles = interval.IntervalSet.between(int(g_first), int(g_last))
    missing_articles = all_articles - article_nums
    LOG.debug(missing_articles)
    for intv in missing_articles:
      articles = nntp.xover_span(group_name, intv.lower_bound, intv.upper_bound)
      for nntp_article in articles:
        queue.put((group_name, [_decode_if_str(f) for f in nntp_article]))
        if exit_event.is_set():
          break
  LOG.info('Finished queueing articles from %s on %s', group_name, server)


def sync(exit_event, config):
  article_queue = Queue.Queue()
  servers = [nntp.NNTP.FromConfig(server) for server in config.get('servers')]

  article_sync = threading.Thread(
    target=ArticleQueueProcessor,
    args=(exit_event, article_queue),
    name='ArticleQueueProcessor')
  article_sync.start()

  for group_name in config.get('groups'):
    sync_thread = threading.Thread(target=QueueArticlesFromServer,
      args=(exit_event, article_queue, group_name, servers[0]),
      name='Sync[%s]' % group_name)
    #sync_thread.daemon = True
    sync_thread.start()

  while threading.active_count() > 1:
    if exit_event.is_set():
      LOG.info('waiting...')
      LOG.debug(threading.enumerate())
      time.sleep(0.5)
    else:
      try:
        time.sleep(10)
      except KeyboardInterrupt as kbd_err:
        exit_event.set()


if __name__ == "__main__":
  config = yaml.load(open('config.yaml'))
  store.LoadMatchers(open(config['regexp_file'], 'rb'))
  exit_event = threading.Event()
  try:
    sync(exit_event, config)
  except KeyboardInterrupt as kbd_err:
    pass



"""
import store
store.LoadMatchers(open('regexp.txt', 'rb'))
for article in store.Article.unmatched().iterator():
  for matcher in store.Matchers:
    match = matcher.pattern.match(article.subject)
    if match:
      article.addSegment(match.groupdict())
      break


"""
