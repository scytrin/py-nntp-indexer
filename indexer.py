#!/usr/bin/python2.7

import BaseHTTPServer
import calendar
import cgi
import ConfigParser
from cStringIO import StringIO
from datetime import datetime, timedelta
import email.utils
import json
import logging
import nntplib
from multiprocessing import BoundedSemaphore, Process, Queue
import random
import signal
import string
import sqlite3
import sys
from threading import Thread
import time
from urlparse import parse_qs, urlparse
import wsgiref
from wsgiref.simple_server import make_server
from wsgiref.util import FileWrapper

sqlite3.enable_callback_tracebacks(True)
logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)



class NNTPCache:
  def __init__(self, config, init=False):
    sqlite3.register_converter("timestamp", lambda b: calendar.timegm(time.strptime(b, '%Y-%m-%d %H:%M:%S')))
    self.db = sqlite3.connect(config.get('indexer', 'cache_file'),
                              detect_types=sqlite3.PARSE_DECLTYPES)
    self.db.text_factory = str
    if init:
      self.db.execute('CREATE TABLE IF NOT EXISTS groups('
                      'watch BOOLEAN NOT NULL DEFAULT 0, '
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

  def add_article(self, subject, group_name, message_id, number):
    LOG.debug((subject, group_name, message_id, number))
    self.add_articles([[ subject, group_name, message_id, number ]])

  def add_articles(self, articles):
    # [ (post_date, subject, message_id, group, number), ... ]
    LOG.debug((len(articles), articles[0]))
    statement1 = 'INSERT OR REPLACE INTO articles VALUES (?, ?, ?)'
    arguments1 = [ a[:3] for a in articles ]
    statement2 = 'INSERT OR REPLACE INTO group_articles VALUES (?, ?, ?)'
    arguments2 = [ a[2:] for a in articles ]
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

  def get_articles(self, query=None, limit=100, offset=0):
    LOG.debug((limit, offset, query))

    if query:
      statement = 'SELECT * FROM articles WHERE subject LIKE ? ORDER BY post_date LIMIT ? OFFSET ?'
      parameters = ['%%%s%%' % str(query), int(limit), int(limit*offset)]
    else:
      statement = 'SELECT * FROM articles LIMIT ? OFFSET ?'
      parameters = [int(limit), int(limit*offset)]

    LOG.debug(statement)
    LOG.debug(parameters)
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, parameters)
    return db_cursor.fetchall()

  def get_last_read(self, group_name):
    LOG.debug((group_name,))
    statement = 'SELECT max(number) FROM group_articles WHERE group_name = ? GROUP BY group_name'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, ( group_name, ))
    result = db_cursor.fetchone()
    return result and result[0] or 0

  def add_group(self, group_name):
    LOG.debug((group_name,))
    self.add_groups([(group_name,)])

  def add_groups(self, group_names):
    # [ (group_name,), ... ]
    #LOG.debug((len(group_names), group_names))
    statement = 'INSERT OR REPLACE INTO groups(group_name) VALUES (?)'
    arguments = group_names
    db_cursor = self.db.cursor()
    while True:
      try:
        db_cursor.executemany(statement, arguments)
        self.db.commit()
        break
      except sqlite3.OperationalError as err:
        LOG.error(err)
        time.sleep(random.randint(1,3))

  def get_groups(self, query=None, limit=100, offset=0):
    LOG.debug((limit, offset, query))

    if query:
      statement = 'SELECT * FROM groups WHERE group_name LIKE ? ORDER BY group_name LIMIT ? OFFSET ?'
      parameters = ['%%%s%%' % str(query), int(limit), int(limit*offset)]
    else:
      statement = 'SELECT * FROM groups ORDER BY group_name LIMIT ? OFFSET ?'
      parameters = [int(limit), int(limit*offset)]

    LOG.debug(statement)
    LOG.debug(parameters)
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, parameters)
    return db_cursor.fetchall()

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
    return [ row[0] for row in db_cursor if row ]



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

    self.action_queue = Queue()
    self.actions = BoundedSemaphore(20)
    self.starter = Thread(target=self.action_starter, name='starter',
                          args=(self.action_queue,))
    self.starter.daemon = True
    self.starter.start()

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
      t = email.utils.parsedate_tz(a[3])
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
          cache.add_articles(map(articles, for_cache))
    except KeyboardInterrupt:
      #LOG.critical('KB INTERRUPT')
      return



class IndexServer:
  @classmethod
  def start(self, config_file='defaults.cfg'):
    indexer = Indexer(config_file)
    host = indexer.config.get('server', 'host')
    port = indexer.config.getint('server', 'port')
    def inject_indexer(environ, start_response):
      environ['indexer'] = indexer
      return self(environ, start_response)
    make_server(host, port, inject_indexer).serve_forever()

  def __init__(self, environ, start_response):
    self.environ = environ
    self.start = start_response
    self.path_info = urlparse(self['PATH_INFO'])
    self.query = parse_qs(self['QUERY_STRING'])
    self._post_params = None

  def __getitem__(self, key):
    return self.environ[key]

  @property
  def indexer(self):
    return self['indexer']

  @property
  def post_params(self):
    if not self._post_params:
      self._post_params = {}
      if self.environ['REQUEST_METHOD'].upper() == 'POST':
        content_type = self.environ.get('CONTENT_TYPE', 'application/x-www-form-urlencoded')
        if (content_type.startswith('application/x-www-form-urlencoded')
            or content_type.startswith('multipart/form-data')):
          try:
            request_body_size = int(self.environ.get('CONTENT_LENGTH', 0))
          except (ValueError):
            request_body_size = 0
          self._post_params = parse_qs(self['wsgi.input'].read(request_body_size))
          LOG.debug(self._post_params)
    return self._post_params

  def params(self, key, *default, **kw):
    which = kw.get('which', 0)
    transform = kw.get('transform', lambda x: x)
    params = map(transform, self.query.get(key, default))
    if which is None:
      return params
    elif len(params) > which:
      return params[which]
    elif len(default) > which:
      return default[which]
    else:
      return None

  def __iter__(self):
    if self['REQUEST_METHOD'] == 'POST' and self.path_info.path == '/':
      return self.process_post()
    elif self['REQUEST_METHOD'] == 'GET':
      func_name = self.path_info.path.lstrip('/').split('/')[0]
      if func_name == '':
        func_name = 'index'
      return getattr(self, func_name, self.err_404)()
    else:
      return err_405()

  def process_post(self):
    self.start('200 OK', [('Content-type', 'text/plain')])
    if self.post_params.get('reload_groups', False):
      self.indexer.update_groups()
      yield "Reloading groups..."
    if self.post_params.get('fetch_articles', False):
      self.indexer.update_watched()
      yield "Fetching articles of watched groups..."
    if 'watch' in self.post_params:
      with self.indexer.cache_connection as cache:
        for group in self.post_params['watch']:
          cache.set_watched(group, True)
          yield "Watching %s" % group
    if 'unwatch' in self.post_params:
      with self.indexer.cache_connection as cache:
        for group in self.post_params['unwatch']:
          cache.set_watched(group, False)
          yield "Not watching %s" % group

  def err_404(self):
    self.start('404 Not Found', [('Content-Type', 'test/plain')])
    yield 'nothing found!!!'

  def err_405(self):
    self.start('405 Method Not Allowed', [('Content-Type', 'test/plain')])
    yield 'Not allowed!'

  def index(self):
    self.start('200 OK', [('Content-type', 'text/html')])
    return FileWrapper(open('index.html'))

  def find_items(self, func):
    query = self.params('q', '', which=-1)
    limit = self.params('l', 100, transform=int)
    offset = self.params('o', 0, transform=int)
    LOG.debug((query, limit, offset))
    return func(query, limit, offset)

  def groups(self):
    with self.indexer.cache_connection as cache:
      groups = self.find_items(cache.get_groups)
    LOG.debug(groups)
    self.start('200 OK', [('Content-type', 'application/json')])
    yield json.dumps(groups)

  def articles(self):
    with self.indexer.cache_connection as cache:
      articles = self.find_items(cache.get_articles)
    self.start('200 OK', [('Content-type', 'application/json')])
    yield json.dumps(articles)



def main():
  if True:
    IndexServer.start()
  else:
    app = Indexer('defaults.cfg')
    app.build_group_list()
    with app.cache_connection as cache:
      cache.set_watched('alt.binaries.tv', True)
      cache.set_watched('alt.binaries.hdtv', True)
    app.update_watched()


if __name__ == '__main__':
  main()
