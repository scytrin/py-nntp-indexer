#!/usr/bin/python

import collections
import datetime
import difflib
import fnmatch
import hashlib
import logging
import re
import time

from dateutil import parser
import peewee
from playhouse import fields
import tinydb
import tinydb.middlewares
import tinydb.operations
import tinydb.serialize
#import tinydb.storages


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
#logging.getLogger('peewee').setLevel(logging.DEBUG)



def AddArticleFromNNTP(group_name, nntp_article):
  #a_no, subject, poster, when, a_id, refs, size, lines = article
  article, a_created = Article.add_from_nntp(group_name, nntp_article)
  group_index, g_created = GroupIndex.add_from_nntp(group_name, nntp_article)
  return
  for matcher in Matcher.for_group(group_name):
    match = matcher.pattern.match(nntp_article[1])
    if match:
      segment, s_created = Segment.add_from_nntp(
        group_name, nntp_article, match.groupdict())


Matcher = collections.namedtuple('Matcher', ['pattern', 'group_glob', 'description'])
Matchers = list()

def AddMatcher(pattern, group_glob, description):
  matcher = Matcher(re.compile('^' + pattern + '$', re.I), group_glob, description)
  if matcher not in Matchers:
    Matchers.append(matcher)

def MatchersForGroup(group_name):
  return [m for m in Matchers if fnmatch.fnmatch(group_name, m.group_glob)]



peewee_db = peewee.SqliteDatabase('articles.db', threadlocals=True)

def setup():
  peewee_db.connect()
  for table in [GroupIndex, Article, Segment]:
    #table.drop_table(fail_silently=True)
    table.create_table(fail_silently=True)

class BaseModel(peewee.Model):
  class Meta:
      database = peewee_db


class Article(BaseModel):
  identifier = peewee.TextField(primary_key=True, null=False)
  poster = peewee.TextField(null=False)
  posted = peewee.DateTimeField(null=False)
  subject = peewee.TextField(null=False)

  def subject_close_matches(self, strings, tolerance=5):
    for match in difflib.get_close_matches(self.subject, strings):
      dist = len([d for d in difflib.ndiff(self.subject, match) if not d.startswith(' ')]) / 2
      if dist <= tolerance:
        LOG.info([dist, match])
        yield dist, match

  @classmethod
  def decode(self, string):
    encodings = ('ascii', 'latin-1', 'cp037')
    for encoding in encodings:
      try:
        return unicode(string, encoding)
      except UnicodeDecodeError:
        LOG.info('Encoding failure: %s %s', string, encoding)
        pass


  @classmethod
  def add_from_nntp(cls, group_name, nntp_article):
    try:
      return cls.create_or_get(**{
        'subject': cls.decode(nntp_article[1]),
        'poster': cls.decode(nntp_article[2]),
        'posted': parser.parse(nntp_article[3]),
        'identifier': nntp_article[4]
      })
    except UnicodeDecodeError as err:
      LOG.error('%s: %s %s', type(err), group_name, repr(nntp_article))
      raise err


class GroupIndex(BaseModel):
  name = peewee.TextField(null=False)
  number = peewee.BigIntegerField(null=False)
  article = peewee.ForeignKeyField(Article, null=False, related_name='group_indexes')

  class Meta:
    primary_key = peewee.CompositeKey('name', 'number')

  @classmethod
  def add_from_nntp(cls, group_name, nntp_article):
    return cls.create_or_get(**{
      'name': group_name,
      'number': int(nntp_article[0]),
      'article': nntp_article[4],
    })

  @classmethod
  def last_for_group(cls, group_name):
    q = cls.select().where(cls.name == group_name)
    return q.aggregate(peewee.fn.Max(cls.number))


class Segment(BaseModel):
  article = peewee.ForeignKeyField(Article, primary_key=True, null=False, related_name='segments')
  release_name = peewee.TextField(null=True)
  file_name = peewee.TextField(null=False)
  file_total = peewee.IntegerField(null=False, default=0)  # 0 is unknown
  file_number = peewee.IntegerField(null=False, default=0)  # 0 is unknown
  part_total = peewee.IntegerField(null=False, default=0)  # 0 is unknown
  part_number = peewee.IntegerField(null=False, default=0)  # 0 is unknown

  def __str__(self):
    segment_str = ' [%i/%i] (%i/%i) "%s">' % (
      self.file_name, self.file_number, self.file_total,
      self.part_number, self.part_total)
    return super(Segment, self).__str__().replace(segment_str[-1], segment_str)

  @classmethod
  def add_from_nntp(cls, group_name, nntp_article, segment_data):
    file_name = segment_data.get('file_name').strip()
    segment, s_created = cls.create_or_get(**{
      'release_name': segment_data.get('release_name', file_name),
      'file_name': file_name,
      'article': nntp_article[4],
      'file_total': int(segment_data.get('file_total', 1)),
      'file_number': int(segment_data.get('file_number', 1)),
      'part_total': int(segment_data.get('part_total', 1)),
      'part_number': int(segment_data.get('part_number', 1)),
    })

  @classmethod
  def release_list(cls):
    q = cls.select(cls.release_name)
    q = q.group_by(cls.release_name)
    q = q.order_by(cls.release_name)
    return [i.release_name for i in q]

  @classmethod
  def release_file_name_list(cls, release_name):
    q = cls.select(cls.file_name)
    q = q.where(cls.release_name == release_name)
    q = q.order_by(cls.file_name)
    return q

  @classmethod
  def release_file_parts(cls, release_name, file_name):
    q = cls.select()
    q = q.where(cls.release_name == release_name)
    q = q.where(cls.file_name == file_name)
    q = q.order_by(cls.file_number, cls.part_number)
    return q

  @classmethod
  def integrity_check(cls, release_name):
    rel_q = cls.select(cls.file_name).where(cls.release_name == release_name)
    for file_segment in rel_q.order_by(cls.file_name):
      file_name = file_segment.file_name
      q = Segment.release_file_parts(release_name, file_name)
      parts = set(s.part_number for s in q.select(Segment.part_number))
      missing = set(xrange(1, q.get().part_total+1)) - parts
      if any(missing):
        return False

  @property
  def release_posted(self):
    q = Segment.select().where(Segment.release_name == self.release_name).join(Article)
    return q.aggregate(peewee.fn.Min(Article.posted))

  @property
  def parts_missing(self):
    q = Segment.release_file_parts(self.release_name, self.file_name)
    parts = set(s.part_number for s in q.select(Segment.part_number))
    return [n+1 for n in xrange(self.part_total) if n+1 not in parts]
