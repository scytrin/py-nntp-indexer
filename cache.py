import datetime
import email.utils
import logging
import peewee
from threading import RLock

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def _rfc2822_to_datetime(value):
  t = email.utils.parsedate_tz(value)
  e = email.utils.mktime_tz(t)
  return datetime.datetime.utcfromtimestamp(e)

def _instr(string, lookup):
  if lookup in string:
    return string.index(lookup)
  else:
    return -1

database = peewee.SqliteDatabase('nntp.db', threadlocals=True)
database.get_conn().create_function('instr', 2, _instr)

class Group(peewee.Model):
  id = peewee.PrimaryKeyField()
  name = peewee.CharField(unique=True)
  watch = peewee.BooleanField(index=True, default=False)

  class Meta:
    database = database

  @classmethod
  def add_from_nntplib(cls, groups):
    # ( (count, first, last, name), ... )
    with database.transaction():
      for group in groups:
        cls.get_or_create(name=group[0])
    LOG.info("Finished loading groups")

  @classmethod
  def watched(cls):
    return list(cls.select().where(cls.watch == True))

  @classmethod
  def watch_set(cls, *groups):
    Group.update(watch=True).where(Group.name << groups).execute()

  @classmethod
  def unwatch_set(cls, *groups):
    Group.update(watch=False).where(Group.name << groups).execute()



class Article(peewee.Model):
  id = peewee.PrimaryKeyField()
  group = peewee.ForeignKeyField(Group)
  number = peewee.IntegerField()
  subject = peewee.TextField(index=True)
  poster = peewee.CharField()
  posted = peewee.DateTimeField()
  message_id = peewee.CharField()
  size = peewee.IntegerField()

  class Meta:
    database = database
    indexes = (
        (('group', 'message_id'), True),
        (('group', 'number'), True)
    )

  @classmethod
  def add_from_nntplib(cls, group_name, articles):
    # ( (a_no, subject, poster, when, a_id, refs, sz, li), ... )
    group = Group.get_or_create(name=group_name)
    LOG.debug((group, articles[0]))
    with database.transaction():
      for article in articles:
        posted = _rfc2822_to_datetime(article[3])
        article = Article.get_or_create(group=group,
                                        number=article[0],
                                        subject=article[1],
                                        poster=article[2],
                                        posted=posted,
                                        message_id=article[4],
                                        size=article[6])


