#!/usr/bin/python2.7

import BaseHTTPServer
import urlparse
import ConfigParser
from cStringIO import StringIO
import logging
import nntplib
from multiprocessing import BoundedSemaphore, Process, Queue
import random
import signal
import sqlite3
import sys
import time

sqlite3.enable_callback_tracebacks(True)
logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)



class NNTPCache:
  def __init__(self, config, init=False):
    self.db = sqlite3.connect(config.get('indexer', 'cache_file'),
                              detect_types=sqlite3.PARSE_DECLTYPES)
    self.db.text_factory = str
    if init:
      self.db.execute('CREATE TABLE IF NOT EXISTS groups('
                      'watch BOOLEAN NOT NULL DEFAULT False, '
                      'group_name NOT NULL, '
                      'UNIQUE(group_name) ON CONFLICT REPLACE)')
      self.db.execute('CREATE TABLE IF NOT EXISTS articles('
                      'post_date TIMESTAMP NOT NULL, '
                      'subject NOT NULL, '
                      'message_id NOT NULL, '
                      'UNIQUE(message_id) ON CONFLICT REPLACE)')
      self.db.execute('CREATE TABLE IF NOT EXISTS group_articles('
                      'message_id NOT NULL, '
                      'group_name NOT NULL, '
                      'number INTEGER NOT NULL, '
                      'UNIQUE(group_name, number) ON CONFLICT REPLACE)')
      self.db.commit()

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.db.close()

  def add_group(self, group_name):
    LOG.debug((group_name,))
    self.add_groups([(group_name,)])

  def add_groups(self, group_names):
    # [ (group_name,), ... ]
    #LOG.debug((len(group_names), group_names))
    statement = 'INSERT OR REPLACE INTO groups VALUES (?, ?)'
    arguments = [ (False, g[0]) for g in group_names]
    db_cursor = self.db.cursor()
    while True:
      try:
        db_cursor.executemany(statement, arguments)
        self.db.commit()
        break
      except sqlite3.OperationalError as err:
        LOG.error(err)
        time.sleep(random.randint(1,3))

  def add_article(self, subject, group_name, message_id, number):
    LOG.debug((subject, group_name, message_id, number))
    self.add_articles([( subject, group_name, message_id, number )])

  def add_articles(self, articles):
    # [ (post_date, subject, message_id, group, number), ... ]
    LOG.debug((len(articles), articles[0]))
    statement1 = 'INSERT OR REPLACE INTO articles VALUES (?, ?)'
    arguments1 = [ a[:2] for a in articles ]
    statement2 = 'INSERT OR REPLACE INTO group_articles VALUES (?, ?, ?)'
    arguments2 = [ a[1:] for a in articles ]
    db_cursor = self.db.cursor()
    while True:
      try:
        db_cursor.executemany(statement1, arguments1)
        db_cursor.executemany(statement2, arguments2)
        self.db.commit()
        break
      except sqlite3.OperationalError as err:
        LOG.error(err)
        time.sleep(random.randint(1,3))

  def get_article(self, message_id):
    LOG.debug((message_id,))
    statement = 'SELECT * FROM articles WHERE message_id = ?'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, (message_id,))
    return db_cursor.fetchone()

  def get_articles(self, group_name='*', start=0, limit=1000, fragment=None):
    LOG.debug((group_name, start, limit))
    statement = 'SELECT * FROM articles %s LIMIT 100;'
    if not fragment:
      db_cursor = self.db.cursor()
      db_cursor.execute(statement % '')
      return db_cursor.fetchall()

    db_cursor = self.db.cursor()
    db_cursor.execute(statement % 'WHERE subject LIKE ?', (fragment, ))
    return db_cursor.fetchall()

  def get_last_read(self, group_name):
    LOG.debug((group_name,))
    statement = 'SELECT max(number) FROM group_articles WHERE group_name = ? GROUP BY group_name'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, ( group_name, ))
    result = db_cursor.fetchone()
    return result and result[0] or 0

  def set_watched(self, group_name, watch=True):
    LOG.debug((group_name, watch))
    statement = 'UPDATE groups SET watch = ? WHERE group_name = ?'
    arguments = ( watch, group_name )
    db_cursor = self.db.cursor()
    while True:
      try:
        db_cursor.execute(statement, arguments)
        self.db.commit()
        break
      except sqlite3.OperationalError as err:
        LOG.error(err)
        time.sleep(random.randint(1,3))

  def get_watched(self):
    statement = 'SELECT group_name FROM groups WHERE watch'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement)
    return [ row[0] for row in db_cursor ]



class NNTPWorker(nntplib.NNTP):
  def __init__(self, semaphore, *args, **kw):
    self.semaphore = semaphore
    self.semaphore.acquire()
    nntplib.NNTP.__init__(self, *args, **kw)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.quit()

  def quit(self):
    nntplib.NNTP.quit(self)
    self.semaphore.release()



class Indexer:
  def __init__(self, config_file='defaults.cfg'):
    self.config = ConfigParser.SafeConfigParser()
    self.config.readfp(open(config_file))

    self.actions = BoundedSemaphore(20)
    self.nntp_c = NNTPCache(self.config, True)
    max_connections = self.config.getint('indexer', 'max_connections')
    self.nntp_semaphore = BoundedSemaphore(max_connections)

  @property
  def nntp_connection(self):
    worker = NNTPWorker(
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
    return NNTPCache(self.config)

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

    LOG.debug('group: %s %d %d', group, last, last_read)

    if last > last_read:
      for start in xrange(last_read, last, span):
        end = min(last, start + span)
        self.start_action(self.fetch_group_articles, (group, start, end),
            'get_group_articles %s %s %s' % (group, start, end))


  # Actions, may be in a process or thread! Beware DB locking!
  def start_action(self, func, args, name=None):
    with self.actions:
      proc = Process(name=name, target=func, args=args)
      proc.daemon = True
    proc.start() # possible deadlock if in the with statement
    return proc

  def build_group_list(self):
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
    try:
      with self.actions:
        with self.nntp_connection as nntp:
          LOG.info("fetching %s: %s - %s ...", group, start, end)
          group_resp = nntp.group(group)
          resp, articles = nntp.xover(str(start), str(end))

        with self.cache_connection as cache:
          LOG.info("storing %s: %s - %s ...", group, start, end)
          # a_no, subject, poster, when, a_id, refs, sz, li
          cache.add_articles([ (a[3], a[1], a[4], group, a[0]) for a in articles ])
    except KeyboardInterrupt:
      #LOG.critical('KB INTERRUPT')
      return



class HTTPServer(BaseHTTPServer.HTTPServer):
  class IndexerView(BaseHTTPServer.BaseHTTPRequestHandler):
    BASE_PAGE_HEAD = '<!DOCTYPE>\n<html><head>\n\t<title>\'dex</title>\n</head><body>'
    BASE_PAGE_FOOT = '</body></html>'
    DIV = '<div>%s</div>\n'

    def header_index(self, out):
      with self.server.indexer.cache_connection as cache:
        articles = cache.get_articles(limit=100, fragment=self.path[1:])
      for article in articles:
        out.write(self.DIV % article[0])

    def do_POST(self):
      url = urlparse.urlparse(self.path)
      LOG.debug(url)
      return


    def do_GET(self):
      self.send_response(200)
      self.send_header('Content-type', 'text/html')
      self.end_headers()
      self.wfile.write(self.BASE_PAGE_HEAD)
      self.wfile.write(
          '''<form onsubmit="window.location.pathname = '/' + this['query'].value; return false;">'''
          '<input name="query" value="' + self.path[1:] + '" />'
          '</form>')
      self.header_index(self.wfile)
      self.wfile.write(self.BASE_PAGE_FOOT)
      return

  def __init__(self, config_file='defaults.cfg'):
    self.indexer = Indexer(config_file)
    self.config = self.indexer.config
    host = self.config.get('server', 'host')
    port = self.config.getint('server', 'port')
    LOG.info('Setting up server on %s:%d', host, port)
    BaseHTTPServer.HTTPServer.__init__(self, (host, port), self.IndexerView)

  def start(self):
    try: self.serve_forever()
    except KeyboardInterrupt: self.socket.close()


def main():
  app = HTTPServer()
  app.start()
  return

  app = indexer.Indexer('defaults.cfg')
  app.build_group_list()
  with app.cache_connection as cache:
    cache.set_watched('alt.binaries.tv', True)
    cache.set_watched('alt.binaries.hdtv', True)
  app.update_watched()


if __name__ == '__main__':
  main()
