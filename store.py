#!/usr/bin/python

import datetime
import difflib
import logging
import time

from dateutil import parser
from third_party import peewee


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

db = peewee.SqliteDatabase('articles.db', threadlocals=True)

class BaseModel(peewee.Model):
  class Meta:
      database = db

class Group(BaseModel):
  name = peewee.TextField(primary_key=True, null=False)

  @property
  def latest_article(self):
    try:
      return self.articles.order_by(Article.number).get()
    except Article.DoesNotExist:
      return None
    finally:
      pass

  @property
  def nzb_articles(self):
    return self.articles.where(Article.subject.contains('nzb'))

  @property
  def matchers(self):
    q = Matcher.select().where(peewee.fn.Like(Matcher.group_name, self.name) | Matcher.group_name >> None)
    LOG.info(q)
    return q

class Matcher(BaseModel):
  group_name = peewee.TextField(null=True, default=None)
  pattern = peewee.TextField(null=False)

  @property
  def groups(self):
    return Group.select().where(peewee.fn.Like(self.group_name, Group.name))

class Article(BaseModel):
  group = peewee.ForeignKeyField(Group, null=False, related_name='articles')
  number = peewee.BigIntegerField(null=False)
  poster = peewee.TextField(null=False)
  posted = peewee.DateTimeField(null=False)
  subject = peewee.TextField(null=False)
  identifier = peewee.TextField(primary_key=True, null=False)

  def subject_close_matches(self, strings, tolerance=5):
    for match in difflib.get_close_matches(self.subject, strings):
      dist = len([d for d in difflib.ndiff(self.subject, match) if not d.startswith(' ')]) / 2
      if dist <= tolerance:
        LOG.info([dist, match])
        yield dist, match

  @classmethod
  def from_nntp(cls, group, article):
    #a_no, subject, poster, when, a_id, refs, sz, li = article
    return cls.create_or_get(
      number=int(article[0]),
      subject=article[1],
      poster=article[2],
      posted=parser.parse(article[3]),
      identifier=article[4],
      group=group)

def setup():
  db.connect()
  db.create_tables([Group, Article, Matcher], safe=True)
