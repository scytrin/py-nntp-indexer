import json
import logging
import os.path
from os.path import commonprefix
import re

from __init__ import Indexer
from store import Group, Article

from third_party import itty
from third_party import gviz_api as gviz

LOG = logging.getLogger('py-usedex.server')

STATIC_FILES = os.path.join(os.path.dirname(__file__), 'static')
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



@itty.get('/groups')
def get_groups(request):
  query = request.GET.get('query', '')
  watched = request.GET.get('watched', '')
  tqx = parse_tqx(request.GET.get('tqx', ''))
  tq = parse_tq(request.GET.get('tq', ''))

  select = Group.select()
  select = select.limit(tq.get('limit', 1))
  select = select.offset(tq.get('offset', 0))
  select = select.order_by(Group.name)
  if query:
    select = select.where(Group.name % ('*%s*' % query))
  if watched:
    select = select.where(Group.watch == True)

  dt = gviz.DataTable({
      'name': ('string', 'Name'),
      'watch': ('boolean', 'Watched')
      })
  dt_order = ['watch', 'name']
  dt.LoadData( g._data for g in select )
  gviz_json = dt.ToJSonResponse(req_id=tqx.get('reqId', 0),
                                columns_order=dt_order)

  return itty.Response(gviz_json, content_type='application/json')

@itty.get('/articles')
def get_articles(request):
  query = request.GET.get('query', '')
  tq = parse_tq(request.GET.get('tq', ''))
  tqx = parse_tqx(request.GET.get('tqx', ''))

  select = Article.select()
  select = select.limit(tq.get('limit', 1))
  select = select.offset(tq.get('offset', 0))
  if query:
    select = select.where(Article.subject % ('*%s*' % query))

  subjects = [ a.subject for a in select ]
  LOG.debug(lcs(subjects))
  LOG.debug(commonprefix(subjects))

  dt = gviz.DataTable({
      'posted': ('datetime', 'Posted'),
      'poster': ('string', 'Poster'),
      'subject': ('string', 'Subject'),
      'message_id': ('string', 'ID')
      })
  dt.LoadData( a._data for a in select )
  dt_order = ['subject', 'posted', 'poster', 'message_id']
  gviz_json = dt.ToJSonResponse(req_id=tqx.get('reqId', 0),
                                columns_order=dt_order)

  return itty.Response(gviz_json, content_type='application/json')

@itty.get('/state')
def get_state(request):
  data = {
      'jobs': IDXR.task_queue.qsize(),
      'articles': Article.select().count(),
      'groups': Group.select().count()
  }
  return itty.Response(json.dumps(data), content_type='application/json')

@itty.get('/')
def index(request):
  return serve(request, 'index.html')

@itty.get('/(?P<filename>[^/]+)')
def serve(request, filename):
  return itty.serve_static_file(request, filename, root=STATIC_FILES)



@itty.post('/rpc/fetch')
def rpc_fetch(request):
  count = int(request.POST.get('count', 1000))
  groups = request.POST.get('group', [])
  if not isinstance(groups, (list, tuple)):
    groups = [ groups ]
  if not groups:
    IDXR.update_watched(count)
  else:
    for group in groups:
      IDXR.get_last(group, count)
  return ''

@itty.post('/rpc/watch')
def rpc_watch(request):
  groups = request.POST.get('group', [])
  if not isinstance(groups, (list, tuple)):
    groups = [ groups ]
  for group in groups:
    Group.update(watch=True).where(Group.name == group).execute()
  return ''

@itty.post('/rpc/unwatch')
def rpc_unwatch(request):
  groups = request.POST.get('group', [])
  if not isinstance(groups, (list, tuple)):
    groups = [ groups ]
  for group in groups:
    Group.update(watch=False).where(Group.name == group).execute()
  return ''

@itty.post('/rpc/reload_groups')
def reload_groups(request):
  IDXR.update_groups()
  return ''



if __name__ == '__main__':
  IDXR = Indexer()
  host = IDXR.config.get('server', 'host')
  port = IDXR.config.getint('server', 'port')
  itty.run_itty(host=host, port=port)
  LOG.info('waiting on %d tasks', IDXR.task_queue.qsize())
  IDXR.task_queue.join()
