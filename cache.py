import logging
import sqlite3
import time

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

sqlite3.enable_callback_tracebacks(True)


class Cache:
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
