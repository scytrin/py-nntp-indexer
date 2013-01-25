import calendar
import datetime
import email.utils
import logging
import random
import sqlite3
from threading import RLock
import time

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def _datetime_to_epoch(value):
  t = value.utctimetuple()
  return calendar.timegm(t)
def _rfc2822_to_datetime(value):
  t = email.utils.parsedate_tz(value)
  e = email.utils.mktime_tz(t)
  return datetime.datetime.utcfromtimestamp(e)
def _sqlite_to_datetime(value):
  try:
    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
  except ValueError as err:
    #LOG.debug(err)
    return _rfc2822_to_datetime(value)

def _dict_row_factory(cursor, row):
  return dict( (c[0], row[i]) for i, c in enumerate(cursor.description) )

sqlite3.enable_callback_tracebacks(True)
sqlite3.register_converter("timestamp", _sqlite_to_datetime)

class Cache:
  def __init__(self, db, init=False):
    self.lock = RLock()
    self.db = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES)
    self.db.text_factory = str
    #self.db.row_factory = _dict_row_factory
    if init:
      self._initialize()

  def _initialize(self):
    with self.lock:
      self.db.interrupt()
      self.db.execute('CREATE TABLE IF NOT EXISTS groups('
                      'watch BOOLEAN NOT NULL DEFAULT 0, '
                      'group_name NOT NULL, '
                      'UNIQUE(group_name) ON CONFLICT REPLACE)')
      self.db.execute('CREATE TABLE IF NOT EXISTS articles('
                      'post_date TIMESTAMP NOT NULL, '
                      'poster NOT NULL, '
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


  def add_articles(self, group, articles):
    # ( (a_no, subject, poster, when, a_id, refs, sz, li), ... )
    LOG.debug((len(articles), articles[0]))
    statement1 = 'INSERT OR REPLACE INTO articles VALUES (?, ?, ?, ?)'
    arguments1 = [ (_rfc2822_to_datetime(a[3]), a[2], a[1], a[4])
                  for a in articles ]
    statement2 = 'INSERT OR REPLACE INTO group_articles VALUES (?, ?, ?)'
    arguments2 = [ (a[4], group, a[0])
                  for a in articles ]
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.executemany(statement1, arguments1)
      db_cursor.executemany(statement2, arguments2)
      self.db.commit()

  def get_article(self, message_id):
    LOG.debug((message_id,))
    statement = 'SELECT * FROM articles WHERE message_id = ?'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement, (message_id,))
    return db_cursor.fetchone()

  def get_articles(self, query=None, offset=0, limit=1000):
    LOG.debug((limit, offset, query))

    if query:
      statement = 'SELECT * FROM articles WHERE subject LIKE ? ORDER BY post_date ASC LIMIT ? OFFSET ?'
      parameters = ['%%%s%%' % str(query), int(limit), int(limit*offset)]
    else:
      statement = 'SELECT * FROM articles ORDER BY post_date DESC LIMIT ? OFFSET ?'
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

  def add_groups(self, groups):
    # ( (count, first, last, name), ... )
    LOG.debug((len(groups), groups[0]))
    statement = 'INSERT OR REPLACE INTO groups VALUES (?, ?)'
    arguments = [ (False, g[0]) for g in groups ]
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.executemany(statement, arguments)
      self.db.commit()

  def get_groups(self, query=None, offset=0, limit=10000):
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
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.execute(statement, arguments)
      self.db.commit()

  def get_watched(self):
    statement = 'SELECT group_name FROM groups WHERE watch'
    db_cursor = self.db.cursor()
    db_cursor.execute(statement)
    return [ row[0] for row in db_cursor if row ]
