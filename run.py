#!/usr/bin/python2

import logging
import sys
import time
import yaml

from nntp import NNTP
import store


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


def load_regexp():
  store.Matcher.delete().execute()
  for line in open('regex', 'rb'):
    group_name, pattern = eval('(' + line + ')')
    if group_name.endswith('.*'):
      group_name = group_name[:-1] + '%'
    elif not group_name:
      group_name = None
    matcher = store.Matcher.create_or_get(
      group_name=group_name, pattern=pattern)[0]


def sync_articles(config):
  with NNTP.Connect(config.get('servers')[0]) as nntp:
    for group_name in config.get('groups'):
      group = store.Group.get_or_create(name=group_name)[0]
      group_resp, count, first, last, name = nntp.group(group.name)
      latest_article = group.latest_article
      if latest_article:
        first = max(latest_article.number, int(first))
      LOG.info('Fetching from %d to %d' % (int(first), int(last)))
      # Look up the most recent article for this group and use over first
      article_count = 0
      for article in nntp.xover_spans_reversed(first, last):
        article_count += 1
        article_inst = store.Article.from_nntp(group, article)
        LOG.info((article_inst[0].number, article_inst[0].subject))
        if article_count >= 1000:
          break


def find_pattern(config):
  for group_name in config.get('groups'):
    group = store.Group.get_or_create(name=group_name)[0]
    LOG.info((group.name, group.articles.count(), group.matchers.count()))
    for matcher in group.matchers:
      q = group.articles.where(store.Article.subject.regexp(matcher.pattern))
      LOG.info(q)
      for article in q:
        LOG.info(article)


def find_diff(config):
  for group_name in config.get('groups'):
    groupings = {}
    group = store.Group.get_or_create(name=group_name)[0]
    for article in group.articles:
      LOG.info([0, article.subject])
      matches = [m for m in article.subject_close_matches(groupings, 3)]
      if len(matches) == 1:
        groupings[matches[0][1]].append(article.number)
        continue
      elif matches:
        LOG.warn([len(matches), article.subject])
        raise RuntimeError('Many close matches!')
        #LOG.debug(matches)
      LOG.warn('NO CLOSE MATCHES FOUND')
      groupings[article.subject] = [article.number]


def subjects(config):
  subjects = set()
  for group_name in config.get('groups'):
    group = store.Group.get_or_create(name=group_name)[0]
    for article in group.nzb_articles.order_by(store.Article.number):
      LOG.info([len(subjects), article.subject])
      matches = [a for a in article.subject_close_matches(subjects, 5)]
      if not matches:
        subjects.add(article.subject)
  for subject in subjects:
    print subject
  print len(subjects)


def main(argv):
  config = yaml.load(open(argv[1]))
  store.setup()
  load_regexp()

  if argv[2] == 'look':
    return subjects(config)
  elif argv[2] == 'sync':
    return sync_articles(config)
  elif argv[2] == 'find':
    return find_pattern(config)


if __name__ == "__main__":
  main(sys.argv)
