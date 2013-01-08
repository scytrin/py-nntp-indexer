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

logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def get_config(config_file='defaults.cfg'):
  config = ConfigParser.SafeConfigParser()
  config.readfp(open(config_file))
  return config

def get_database(db_file, init=False):
  db = sqlite3.connect('cache.db')
  if init:
    db.execute('CREATE TABLE IF NOT EXISTS articles('
      'a_id NOT NULL, '
      'group_name NOT NULL, '
      'subject NOT NULL, '
      'headers)')
    db.commit()
  return db

def get_groups(config):
  groups = config.get('indexer', 'groups').split(',')
  return groups

def get_nntp_connection(config):
  LOG.info('starting new nntp connection')
  nntp = NNTP(
    config.get('indexer', 'host'),
    config.getint('indexer', 'port'),
    config.get('indexer', 'username'),
    config.get('indexer', 'password'))
  nntp.set_debuglevel(1)
  return nntp




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
    LOG.info("storing %s %s - %s ...", group, start, end)
    for a_no, subject, poster, when, a_id, refs, sz, li in articles:
      store_article(config, a_id, group, subject)

  LOG.info("fetched %s %s - %s ...", group, start, end)
  return articles


def store_article(config, a_id, group, subject):
  database = get_database(config)
  db_cursor = database.cursor()

  db_cursor.execute('select * from articles where a_id = ?', (a_id,))
  cached = db_cursor.fetchone()
  LOG.debug('cached: %s %s', a_id, bool(cached))
  if cached: return

  db_cursor.execute('insert into articles values (?, ?, ?, ?)',
      ( a_id, group, subject, a_id ))
  database.commit()

def init_worker():
  signal.signal(signal.SIGINT, signal.SIG_IGN)

def main():
  config = get_config()
  groups = get_groups(config)
  dstore = get_database(config, True)
  max_connections = config.getint('indexer', 'max_connections')
  xover_span = config.getint('indexer', 'xover_range')

  for group in groups:
    nntp = get_nntp_connection(config)
    resp, count, first, last, name = nntp.group(group)
    nntp.quit()

    results = []
    try:
      nntp_p = Pool(max_connections, init_worker, maxtasksperchild=1)
      LOG.info('%s with %s : %s-%s', group, count, first, last)
      for start in xrange(int(last), int(first), 0-xover_span):
        results.append( nntp_p.apply_async(group_xover, (config, group, start - xover_span, start)) )
      nntp_p.close()
      nntp_p.join()
    except KeyboardInterrupt:
      nntp_p.terminate()
      nntp_p.join()

    for result in results:
      try:
        LOG.debug(result.get(1))
      except multiprocessing.TimeoutError as err:
        LOG.error(err)

if __name__ == "__main__":
  main()
