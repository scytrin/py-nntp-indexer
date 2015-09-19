#!/usr/bin/python2

import itertools
import logging
from pprint import pformat
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


def decode_if_str(string):
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


def sync_articles(server, group_name, write_lock, stop_event):
  LOG.info('Sync starting for %s on %s', group_name, server)
  with server as nntp:
    group_resp, count, first, last, name = nntp.group(group_name)
    # Look up the most recent article for this group and use over first
    first = max(int(first), store.GroupIndex.last_for_group(group_name))
    article_count = 0
    articles = nntp.xover_span(group_name, first, last)
    for nntp_article in articles:
      nntp_article = [decode_if_str(f) for f in nntp_article]

      with store.peewee_db.atomic():
        with write_lock:
          article = store.Article.addFromNNTP(nntp_article)
        with write_lock:
          article.addGroupIndex(group_name, nntp_article[0])
          for match in article.getSegmentData():
            with write_lock:
              article.addSegment(match)
              break

      if stop_event.is_set():
        break
  LOG.info('Sync finished for %s on %s', group_name, server)


def sync(config):
  sync_threads = set()
  stop_sync_event = threading.Event()
  servers = [nntp.NNTP.FromConfig(server) for server in config.get('servers')]
  for group_name in config.get('groups'):
    sync_name = 'Sync[%s:%s]' % (servers[0].host, group_name)
    sync_thread = threading.Thread(target=sync_articles,
      args=(servers[0], group_name, store.peewee_lock, stop_sync_event),
      name=sync_name)
    #sync_thread.daemon = True
    sync_thread.start()
    sync_threads.add(sync_thread)

  try:
    while any(thread.is_alive() for thread in sync_threads):
      time.sleep(10)
  except KeyboardInterrupt as kbd_err:
    stop_sync_event.set()
    while any(thread.is_alive() for thread in sync_threads):
      LOG.info('waiting...')
      time.sleep(0.5)


def main(argv):
  try:
    config = yaml.load(open('config.yaml'))
    store.LoadMatchers(open(config['regexp_file'], 'rb'))
    for m in store.Matchers:
      print m.description, m.pattern.pattern
    #sync(config)
  except KeyboardInterrupt as kbd_err:
    pass



if __name__ == "__main__":
  main(sys.argv)



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