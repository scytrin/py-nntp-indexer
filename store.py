#!/usr/bin/python

import collections
import datetime
import fnmatch
import hashlib
import logging
import re
import threading
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


MatcherMacros = {
  'size': r'\d+(?:[.]\d+)? MBytes|\d+ [Bb]ytes',
  'comment': r'(?P<comment>.+?)',
  'release': r'(?P<release_name>.+?)',
  'seperator': r'(?:-|\||\|\|)',
  'parts_p': r'\((?P<part_number>\d+)(?:\/| ?of ?)(?P<part_total>\d+)\)',
  'parts_b': r'\[(?P<part_number>\d+)(?:\/| ?of ?)(?P<part_total>\d+)\]',
  'files_b': r'\[(?P<file_number>\d+)(?:\/| ?of ?)(?P<file_total>\d+)\]',
  'files': r'(?P<file_number>\d+)(?:\/| ?of ?)(?P<file_total>\d+)',
  'parts': r'(?P<part_number>\d+)(?:\/| ?of ?)(?P<part_total>\d+)',
  'file_name_parts': r'(?P<file_name>.+\.part(?P<file_number>\d+)\.rar)',
  'file_name': r'(?P<file_name>[^"]+)',
}
Matcher = collections.namedtuple('Matcher', ['pattern', 'description'])
Matchers = list()

def LoadMatchers(iterable):
  for idx, line in enumerate(iterable):
    if line.strip() and not line.startswith('#'):
      pattern = line.strip().format(**MatcherMacros)
      LOG.info((idx, pattern))
      matcher = Matcher(re.compile('^' + pattern + '$', re.I), str(idx+1))
      if matcher not in Matchers:
        Matchers.append(matcher)


peewee_lock = threading.RLock()
peewee_db = peewee.SqliteDatabase('nntp.db', threadlocals=True)


class BaseModel(peewee.Model):
  class Meta:
    database = peewee_db


class Article(BaseModel):
  identifier = peewee.TextField(primary_key=True, null=False)
  poster = peewee.TextField(null=False)
  posted = peewee.DateTimeField(null=False)
  subject = peewee.TextField(null=False)
  size = peewee.BigIntegerField(null=False, default=0)

  @classmethod
  def unmatched(cls):
    unmatched = cls.select().join(Segment, peewee.JOIN.LEFT_OUTER)
    return unmatched.where(Segment.article == None)

  @classmethod
  def addFromNNTP(cls, nntp_article):
    #a_no, subject, poster, when, a_id, refs, size, lines = article
    return Article.create_or_get(
      subject=nntp_article[1],
      poster=nntp_article[2],
      posted=parser.parse(nntp_article[3]),
      identifier=nntp_article[4],
      size=int(nntp_article[6]))[0]

  def addGroupIndex(self, name, number):
    return GroupIndex.create_or_get(
      name=name,
      number=int(number),
      article=self)[0]

  def getSegmentData(self):
    for matcher in Matchers:
      match = matcher.pattern.match(self.subject)
      if match:
        yield match.groupdict()

  def addSegment(self, segment_data):
    file_name = segment_data.get('file_name', '').strip()
    return Segment.create_or_get(
      article=self,
      file_name=file_name,
      release_name=segment_data.get('release_name', file_name),
      file_total=int(segment_data.get('file_total', 0)),
      file_number=int(segment_data.get('file_number', 0)),
      part_total=int(segment_data.get('part_total', 0)),
      part_number=int(segment_data.get('part_number', 0)))[0]


class GroupIndex(BaseModel):
  server = peewee.TextField(null=False)
  name = peewee.TextField(null=False)
  number = peewee.BigIntegerField(null=False)
  article = peewee.ForeignKeyField(Article, null=False, related_name='group_indexes')

  class Meta:
    primary_key = peewee.CompositeKey('server', 'name', 'number')

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
  def integrity_check(cls, release_name):
    rel_q = cls.select(cls.file_name).where(cls.release_name == release_name)
    rel_qo = rel_qo.order_by(cls.file_name, cls.part_number)
    integrity = {}
    for file_part_segment in rel_q.iterator():
      fil_q = rel_q.where(cls.file_name == file_segment.file_name)
      fil_qo = fil_q.order_by(cls.file_number, cls.part_number)
      missing = set(xrange(1, q.get().part_total+1))
      for part_segment in fil_qo.iterator():
        missing.discard(part_segment.part_number)
      if missing:
        LOG.info('Missing parts for "%s": %s', file_segment.file_name)
        return False

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

  @property
  def release_posted(self):
    q = Segment.select().where(Segment.release_name == self.release_name).join(Article)
    return q.aggregate(peewee.fn.Min(Article.posted))

  @property
  def parts_missing(self):
    q = Segment.release_file_parts(self.release_name, self.file_name)
    parts = set(s.part_number for s in q.select(Segment.part_number))
    return [n+1 for n in xrange(self.part_total) if n+1 not in parts]


for table in (Article, GroupIndex, Segment):
  peewee_db.connect()
  table.create_table(fail_silently=True)
  