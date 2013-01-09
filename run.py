#!/usr/bin/python2.7

import ConfigParser
import logging
import multiprocessing
from multiprocessing import Pool, Process
from nntplib import NNTP
import signal
import StringIO
import sqlite3
from Queue import Queue
import thread
import threading
import time

from indexer import Indexer

logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def gather_articles(config, group_name, a_no_1, a_no_2):
  a_no_1, a_no_2 = int(a_no_1), int(a_no_2)
  a_max, a_min = max(a_no_1, a_no_2), min(a_no_1, a_no_2)
  xover_span = min(a_max - a_min, config.getint('indexer', 'xover_range'))
  starts = xrange(a_max, a_min, 0-xover_span)
  max_connections = config.getint('indexer', 'max_connections')

  results = []
  try:
    LOG.info('starting fetch... %s %s', group_name, starts)
    nntp_p = Pool(max_connections, init_worker, maxtasksperchild=1)
    blah = raw_input("Go?")
    for start in starts:
      results.append( nntp_p.apply_async(group_xover, (config, group_name, start - xover_span, start)) )
    nntp_p.close()
    nntp_p.join()
  except KeyboardInterrupt:
    nntp_p.terminate()
    nntp_p.join()

  for result in results:
    LOG.debug(result)
    try:
      LOG.debug(result.get(1))
    except multiprocessing.TimeoutError as err:
      LOG.error(err)

def group_xover(config, group, start, end):
  start = str(start)
  end = str(end)

  nntp = get_nntp_connection(config)
  LOG.info("fetching %s %s - %s ...", group, start, end)
  group_resp = nntp.group(group)
  LOG.debug(group_resp)
  resp, articles = nntp.xover(start, end)
  LOG.debug(articles)
  nntp.quit()

  if articles:
    cache = NNTPCache(config)
    LOG.info("storing %s %s - %s ...", group, start, end)
    for a_no, subject, poster, when, a_id, refs, sz, li in articles:
      cache.add_article(a_id, subject, group)

  LOG.info("fetched %s %s - %s ...", group, start, end)
  cache.set_group_last_read(group, int(end))


def main():
  indexer = Indexer('defaults.cfg')
  #indexer.build_group_list()
  indexer.update_watched()

if __name__ == "__main__":
  main()
