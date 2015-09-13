#!/usr/bin/python2

import itertools
import logging
from pprint import pformat
import re
import sys
import threading
import time
import yaml

import nntp
import store



logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def load_regexp(regexp_file):
  mappings = {
    'comment': r'(?P<comment>.+?)',
    'release': r'(?P<release_name>.+?)',
    'yenc': r'yEnc',
    'seperator': r'(?:-|\||\|\|)',
    'parts_p': r'\((?P<part_number>\d+)(?:\/| of )(?P<part_total>\d+)\)',
    'parts_b': r'\[(?P<part_number>\d+)(?:\/| of )(?P<part_total>\d+)\]',
    'files_b': r'\[(?P<file_number>\d+)(?:\/| of )(?P<file_total>\d+)\]',
    'file_name_parts_q': r'"(?P<file_name>.+\.part(?P<file_number>\d+)\.rar)"',
    'file_name': r'(?P<file_name>[^"]+)',
  }
  for lineno, line in enumerate(open(regexp_file, 'rb')):
    if line.strip() and not line.startswith('#'):
      pattern = line.strip().format(**mappings)
      m = store.AddMatcher(pattern, group_glob='*', description=str(lineno+1))


def sync_articles(server, group_name, stop_event):
  LOG.info((server, group_name))
  with server.connection as connection:
    group_resp, count, first, last, name = connection.group(group_name)
    # Look up the most recent article for this group and use over first
    first = max(int(first), store.GroupIndex.last_for_group(group_name))
    article_count = 0
    articles = connection.xover_span(group_name, first, last)
    while not stop_event.is_set():
      nntp_article = articles.next()
      store.AddArticleFromNNTP(group_name, nntp_article)


def sync(config):
  servers = [nntp.Factory(server) for server in config.get('servers')]
  sync_threads = set()
  stop_sync_event = threading.Event()
  for group_name in config.get('groups'):
    sync_name = 'Sync[%s:%s]' % (servers[0].host, group_name)
    sync_thread = threading.Thread(target=sync_articles,
      args=(servers[0], group_name, stop_sync_event),
      name=sync_name)
    sync_thread.daemon = True
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


def match():
  for entry in store.GroupIndex.select().join(store.Article):
    for matcher in store.MatchersForGroup(entry.name):
      match = matcher.pattern.match(entry.article.subject)
      if match:
        #print out
        break
    if not match:
      LOG.debug('NO MATCH! %s', entry.article.subject)


def main(argv):
  try:
    config = yaml.load(open('config.yaml'))
    store.setup()
    load_regexp(config['regexp_file'])

    #sync(config)
    match()

  except KeyboardInterrupt as kbd_err:
    pass



if __name__ == "__main__":
  main(sys.argv)
