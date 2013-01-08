import sqlite3

class NNTPCache:
    def __init__(self, config, init=False):
        self.db = sqlite3.connect('cache.db')
        if init:
          self.db.execute('CREATE TABLE IF NOT EXISTS articles('
              'group_name NOT NULL, '
              'subject NOT NULL, '
              'article_id UNIQUE NOT NULL)')
          self.db.execute('CREATE TABLE IF NOT EXISTS groups('
              'watch BOOLEAN NOT NULL DEFAULT False, '
              'last_read INTEGER NOT NULL DEFAULT 0, '
              'group_name UNIQUE NOT NULL)')
          self.db.commit()

    def add_group(self, group_name, last=0, watched=False):
      db_cursor = self.db.cursor()
      db_cursor.execute('INSERT OR REPLACE INTO groups VALUES (?, ?, ?)',
          ( bool(watched), int(last), group_name ))
      self.db.commit()

    def set_group_last_read(self, group_name, last_read):
      db_cursor = self.db.cursor()
      db_cursor.execute('UPDATE groups SET last_read = ? WHERE group_name = ? AND last_read < ?',
          ( last_read, group_name, last_read ))
      self.db.commit()

    def get_group_last_read(self, group_name):
      db_cursor = self.db.cursor()
      db_cursor.execute('SELECT last_read FROM groups WHERE group_name = ?',
          ( group_name, ))
      return db_cursor.fetchone()[0]
        
    def add_article(self, article_id, subject, group_name):
      db_cursor = self.db.cursor()
      db_cursor.execute('INSERT OR REPLACE INTO articles VALUES (?, ?, ?)',
          ( article_id, subject, group_name ))
      self.db.commit()
    
    def get_article(self, article_id):
      db_cursor = self.db.cursor()
      db_cursor.execute('SELECT * FROM articles WHERE article_id = ?', (article_id,))
      return db_cursor.fetchone()

    def get_watched(self):
      db_cursor = self.db.cursor()
      db_cursor.execute('SELECT group_name FROM groups WHERE watch')
      return [ row[0] for row in db_cursor ]
