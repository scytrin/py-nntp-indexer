import calendar
import datetime
import email.utils
import logging
import random
import sql
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
    if init:
      self._initialize()

    sql.Flavor.set(sql.Flavor(paramstyle='qmark', max_limit=100000))
    self.articles = sql.Table('articles')
    self.groups = sql.Table('groups')
    self.group_articles = sql.Table('group_articles')

  def _initialize(self):
    with self.lock:
      self.db.interrupt()
      self.db.executescript("""
BEGIN;

CREATE TABLE IF NOT EXISTS groups(
  watch BOOLEAN NOT NULL DEFAULT 0,
  group_name PRIMARY KEY NOT NULL
);

CREATE TABLE IF NOT EXISTS articles(
  post_date TIMESTAMP NOT NULL,
  poster NOT NULL,
  subject NOT NULL,
  message_id PRIMARY KEY NOT NULL
);
CREATE INDEX IF NOT EXISTS article_subject \
  ON articles(subject);

CREATE TABLE IF NOT EXISTS group_articles(
  message_id NOT NULL,
  group_name NOT NULL,
  number INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS group_article \
  ON group_articles(group_name, message_id);
CREATE UNIQUE INDEX IF NOT EXISTS group_number \
  ON group_articles(group_name, number);

COMMIT;
""")

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.db.close()

  _columns_SQL = "PRAGMA table_info(%s)"
  def columns(self, table):
    db_cursor = self.db.cursor()
    db_cursor.execute(Cache._columns_SQL % table)
    return [ row[1] for row in db_cursor.fetchall() ]

  _article_count_SQL = 'SELECT COUNT(*) FROM articles'
  def article_count(self):
    return self.db.execute(Cache._article_count_SQL).fetchone()[0]

  _group_count_SQL = 'SELECT COUNT(*) FROM groups'
  def group_count(self):
    return self.db.execute(Cache._group_count_SQL).fetchone()[0]

  _add_articles_articles_SQL = 'INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?)'
  _add_articles_group_articles_SQL = 'INSERT OR REPLACE INTO group_articles VALUES (?, ?, ?)'
  def add_articles(self, group, articles):
    # ( (a_no, subject, poster, when, a_id, refs, sz, li), ... )
    LOG.debug((len(articles), articles[0]))
    arguments1 = [ (_rfc2822_to_datetime(a[3]), a[2], a[1], a[4])
                  for a in articles ]
    arguments2 = [ (a[4], group, a[0])
                  for a in articles ]
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.executemany(Cache._add_articles_articles_SQL, arguments1)
      db_cursor.executemany(Cache._add_articles_group_articles_SQL, arguments2)
      self.db.commit()

  def get_articles(self, **kwargs):
    articles = sql.Table('articles')
    select = articles.select(
        limit=kwargs.get('limit', 100),
        offset=kwargs.get('offset', 0),
        order_by=articles.post_date)

    if 'subject' in kwargs and kwargs['subject']:
      subject = '%%%s%%' % kwargs['subject']
      if select.where:
        select.where &= articles.subject.like(subject)
      else:
        select.where = articles.subject.like(subject)

    LOG.debug(tuple(select))
    LOG.debug(kwargs)
    db_cursor = self.db.cursor()
    db_cursor.execute(*select)
    return db_cursor.fetchall()

  _get_last_read_SQL = ('SELECT max(number) FROM group_articles '
                        'WHERE group_name = ? GROUP BY group_name')
  def get_last_read(self, group_name):
    LOG.debug((group_name,))
    db_cursor = self.db.cursor()
    db_cursor.execute(Cache._get_last_read_SQL, (group_name,))
    result = db_cursor.fetchone()
    return result and result[0] or 0

  _add_groups_SQL = 'INSERT OR IGNORE INTO groups VALUES (?, ?)'
  def add_groups(self, groups):
    # ( (count, first, last, name), ... )
    LOG.debug((len(groups), groups[0]))
    arguments = [ (False, g[0]) for g in groups ]
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.executemany(Cache._add_groups_SQL, arguments)
      self.db.commit()

  def get_groups(self, **kwargs):
    groups = sql.Table('groups')
    select = groups.select(
        limit=kwargs.get('limit', 100),
        offset=kwargs.get('offset', 0),
        order_by=groups.group_name)

    if 'watch' in kwargs and kwargs['watch']:
      if select.where:
        select.where &= groups.watch
      else:
        select.where = groups.watch

    if 'group_name' in kwargs and kwargs['group_name']:
      group_name = '%%%s%%' % kwargs['group_name']
      if select.where:
        select.where &= groups.group_name.like(group_name)
      else:
        select.where = groups.group_name.like(group_name)

    LOG.debug(tuple(select))
    LOG.debug(kwargs)
    db_cursor = self.db.cursor()
    db_cursor.execute(*select)
    return db_cursor.fetchall()

  _set_watched_SQL = 'UPDATE groups SET watch = ? WHERE group_name = ?'
  def set_watched(self, group_name, watch=True):
    with self.lock:
      self.db.interrupt()
      db_cursor = self.db.cursor()
      db_cursor.execute(Cache._set_watched_SQL, ( watch, group_name ))
      self.db.commit()

  _get_watched_SQL = 'SELECT group_name FROM groups WHERE watch'
  def get_watched(self):
    db_cursor = self.db.cursor()
    db_cursor.execute(Cache._get_watched_SQL)
    return [ row[0] for row in db_cursor if row ]
