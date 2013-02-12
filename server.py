import gviz_api
import flask
from flask import Flask, request
import json
import logging
from os.path import commonprefix
import re

from __init__ import Indexer
from cache_peewee import Group, Article

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

IDXR = None

def parse_tq(tq_str):
  def prs(tq, stop, hsh, key, flag, regex, transform=None):
    new_stop = stop
    try:
      new_stop = tq_str[:stop].rindex(flag)
      match = re.search(regex, tq_str[new_stop:stop])
      if match:
        hsh[key] = transform(match) if transform else match.group(1)
    except ValueError:
      pass
    return new_stop

  query = {}
  stop = len(tq_str) # going backwards...
  stop = prs(tq_str, stop, query, 'offset', 'offset', 'offset (\d+)',
             lambda m: int(m.group(1)))
  stop = prs(tq_str, stop, query, 'limit', 'limit', 'limit (\d+)',
             lambda m: int(m.group(1)))
  stop = prs(tq_str, stop, query, 'order', 'order by', 'order by (.+)',
             lambda m: m.group(1).replace('`',''))

  LOG.debug((query, stop))
  return query

def parse_tqx(tqx_str):
  if not tqx_str:
    return dict()
  return dict( pair.split(':', 2) for pair in tqx_str.split(';') )

def lcs(data):
  substr = ''
  if len(data) > 1 and len(data[0]) > 0:
    for i in range(len(data[0])):
      for j in range(len(data[0])-i+1):
        if j > len(substr) and all(data[0][i:i+j] in x for x in data):
          substr = data[0][i:i+j]
  return substr


app = Flask(__name__)

@app.route('/groups')
def get_groups():
  query = request.args.get('query', '')
  watched = request.args.get('watched', '')
  tqx = parse_tqx(request.args.get('tqx', ''))
  tq = parse_tq(request.args.get('tq', ''))

  select = Group.select()
  select = select.limit(tq.get('limit', 100))
  select = select.offset(tq.get('offset', 0))
  select = select.order_by(Group.name)
  if query:
    select = select.where(Group.name % ('*%s*' % query))
  if watched:
    select = select.where(Group.watch == True)

  dt = gviz_api.DataTable({
      'name': ('string', 'Name'),
      'watch': ('boolean', 'Watched')
      })
  dt_order = ['watch', 'name']
  dt.LoadData( g._data for g in select )
  gviz_json = dt.ToJSonResponse(req_id=tqx.get('reqId', 0),
                                columns_order=dt_order)

  response = flask.make_response(gviz_json)
  response.headers['Content-Type'] = 'application/json'
  return response

@app.route('/articles')
def get_articles():
  query = request.args.get('query', '')
  tq = parse_tq(request.args.get('tq', ''))
  tqx = parse_tqx(request.args.get('tqx', ''))

  select = Article.select()
  select = select.limit(tq.get('limit', 100))
  select = select.offset(tq.get('offset', 0))
  if query:
    select = select.where(Article.subject % ('*%s*' % query))

  subjects = [ a.subject for a in select ]
  LOG.debug(lcs(subjects))
  LOG.debug(commonprefix(subjects))

  dt = gviz_api.DataTable({
      'posted': ('datetime', 'Posted'),
      'poster': ('string', 'Poster'),
      'subject': ('string', 'Subject'),
      'message_id': ('string', 'ID')
      })
  dt.LoadData( a._data for a in select )
  dt_order = ['subject', 'posted', 'poster', 'message_id']
  gviz_json = dt.ToJSonResponse(req_id=tqx.get('reqId', 0),
                                columns_order=dt_order)

  response = flask.make_response(gviz_json)
  response.headers['Content-Type'] = 'application/json'
  return response

@app.route('/state')
def get_state():
  return flask.jsonify(jobs=IDXR.task_queue.qsize(),
                       articles=Article.select().count(),
                       groups=Group.select().count())

@app.route('/<filename>')
def serve(filename):
  return flask.send_from_directory('static', filename)

@app.route('/')
def index():
  return serve('index.html')

@app.route('/rpc/<method>', methods=['POST'])
def rpc_call(method):
  LOG.debug(method)
  LOG.debug(request.form)

  if method == 'reload_groups':
    IDXR.update_groups()

  elif method == 'fetch':
    groups = request.form.getlist('group')
    count = request.form.get('count', 1000, type=int)
    if not groups:
      IDXR.update_watched()
    else:
      for group in groups:
        IDXR.get_last(group, count)

  elif method == 'watch':
    for group in request.form.getlist('group'):
      Group.update(watch=True).where(Group.name == group).execute()

  elif method == 'unwatch':
    for group in request.form.getlist('group'):
      Group.update(watch=False).where(Group.name == group).execute()

  return ''


if __name__ == '__main__':
  IDXR = Indexer()
  if not Group.table_exists():
    Group.create_table()
  if not Article.table_exists():
    Article.create_table()
  host = IDXR.config.get('server', 'host')
  port = IDXR.config.getint('server', 'port')
  app.run(host, port)
