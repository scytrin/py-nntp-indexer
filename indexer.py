import BaseHTTPServer
import ConfigParser
from cStringIO import StringIO
import logging
import nntplib
import signal
import sqlite3



sqlite3.enable_callback_tracebacks(True)
logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)



class NNTPCache:
  def __init__(self, config, init=False):
    self.db = sqlite3.connect(config.get('indexer', 'cache_file'))
    self.db.text_factory = str
    if init:
      self.db.execute('CREATE TABLE IF NOT EXISTS groups('
                      'watch BOOLEAN NOT NULL DEFAULT False, '
                      'group_name NOT NULL, '
                      'UNIQUE(group_name) ON CONFLICT REPLACE)')
      self.db.execute('CREATE TABLE IF NOT EXISTS articles('
                      'subject NOT NULL, '
                      'message_id NOT NULL, '
                      'UNIQUE(message_id) ON CONFLICT REPLACE)')
      self.db.execute('CREATE TABLE IF NOT EXISTS group_articles('
                      'message_id NOT NULL, '
                      'group_name NOT NULL, '
                      'number INTEGER NOT NULL, '
                      'UNIQUE(group_name, number) ON CONFLICT REPLACE)')
      self.db.commit()

  def add_group(self, group_name):
    LOG.debug((group_name,))
    self.add_groups([(group_name,)])

  def add_groups(self, group_names):
    # [ (group_name,), ... ]
    LOG.debug((len(articles), articles))
    statement = 'INSERT OR REPLACE INTO groups VALUES (?, ?)'
    db_cursor = self.db.cursor()
    db_cursor.executemany(statement, [ (False, g) for g in group_names])
    self.db.commit()

  def add_article(self, subject, group_name, message_id, number):
    LOG.debug((subject, group_name, message_id, number))
    self.add_articles([( subject, group_name, message_id, number )])

  def add_articles(self, articles):
    # [ (subject, message_id, group, number), ... ]
    LOG.debug((len(articles), articles[0]))
    db_cursor = self.db.cursor()
    statement1 = 'INSERT OR REPLACE INTO articles VALUES (?, ?)'
    statement2 = 'INSERT OR REPLACE INTO group_articles VALUES (?, ?, ?)'
    db_cursor.executemany(statement1, [ a[:2] for a in articles ])
    db_cursor.executemany(statement2, [ a[1:] for a in articles ])
    self.db.commit()

  def get_article(self, message_id):
    LOG.debug((message_id,))
    statement = 'SELECT * FROM articles WHERE message_id = ?'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, (message_id,))
    return db_cursor.fetchone()

  def get_articles(self, group_name='*', start=0, limit=1000):
    LOG.debug((group_name, start, limit))
    statement = 'SELECT * FROM articles LIMIT 100;'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement)
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
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, ( watch, group_name ))
    self.db.commit()

  def get_watched(self):
    statement = 'SELECT group_name FROM groups WHERE watch'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement)
    return [ row[0] for row in db_cursor ]



class NNTP(nntplib.NNTP):
  def __enter__(self):
    return self
  def __exit__(self, type, value, traceback):
    self.quit()



class HTTPServer(BaseHTTPServer.HTTPServer):
  class IndexerView(BaseHTTPServer.BaseHTTPRequestHandler):
    BASE_PAGE = '<!DOCTYPE>\n<html><head>\n\t<title>\'dex</title>\n</head><body>%s</body></html>'
    ARTICLE = '<div>%s</div>'
    def header_index(self):
      cache = self.server.cache
      buf = StringIO()
      for article in cache.get_articles(limit=100):
        buf.write(self.ARTICLE % article[0])
      return self.BASE_PAGE % buf.getvalue()

    def do_GET(self):
      self.send_response(200)
      self.send_header('Content-type', 'text/html')
      self.end_headers()
      self.wfile.write(self.header_index())
      return

  def __init__(self, config):
    self.config = config
    host = self.config.get('server', 'host')
    port = self.config.getint('server', 'port')
    BaseHTTPServer.HTTPServer.__init__(self, (host, port), self.IndexerView)

  @property
  def cache(self):
    return NNTPCache(self.config)

  def start(self):
    try: self.serve_forever()
    except KeyboardInterrupt: self.socket.close()



class Indexer:
  def __init__(self, config_file='defaults.cfg'):
    self.config = ConfigParser.SafeConfigParser()
    self.config.readfp(open(config_file))
    self.max_connections = self.config.getint('indexer', 'max_connections')
    self.cache = NNTPCache(self.config, True)

  def _new_connection(self):
    nntp = NNTP(
        self.config.get('indexer', 'host'),
        self.config.get('indexer', 'port'),
        self.config.get('indexer', 'username'),
        self.config.get('indexer', 'password'),
        True)
    nntp.set_debuglevel(1)
    return nntp

  def _new_http_server(self):
    http = HTTPServer(self.config)
    return http

  @property
  def http(self):
    return self._new_http_server()

  @property
  def nntp(self):
    return self._new_connection()

  def build_group_list(self):
    with self.nntp as nntp:
      resp, group_list = nntp.list()
    self.cache.add_groups([ (g[0],) for g in group_list ])

  def update_watched(self):
    apply(self.update_group, self.cache.get_watched())

  def update_group(self, group):
    last = 0
    last_read = self.cache.get_last_read(group)
    span = self.config.getint('indexer', 'xover_range')
    with self.nntp as nntp:
      resp, count, first, last, group_name = nntp.group(group)
      last_read = max(int(first), last_read)
      last = int(last)

    LOG.debug('group: %s %d %d', group, last, last_read)

    if last > last_read:
      for start in xrange(last_read, last, span):
        end = min(last, start + span)
        articles = self.get_group_articles(group, start, end)
        LOG.debug('articles %s %s', group, len(articles))
        # a_no, subject, poster, when, a_id, refs, sz, li
        self.cache.add_articles([ (a[1], a[4], group, a[0]) for a in articles ])

  def get_group_articles(self, group, start, end):
    LOG.info("fetching %s: %s - %s ...", group, start, end)
    with self.nntp as nntp:
      group_resp = nntp.group(group)
      resp, articles = nntp.xover(str(start), str(end))
      return articles

def init_worker():
  signal.signal(signal.SIGINT, signal.SIG_IGN)


def main():
  indexer = Indexer('defaults.cfg')
  indexer.http.start()
  #indexer.build_group_list()
  #indexer.update_watched()
