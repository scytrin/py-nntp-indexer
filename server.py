import gviz_api
import itty
import json
import logging
from os.path import commonprefix
import re

from __init__ import Indexer

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

  with IDXR.cache_connection as cache:
    groups = cache.get_groups(group_name=query, watch=watched, **tq)

  dt = gviz_api.DataTable([
      ('watch', 'boolean', 'Watched'),
      ('group_name', 'string', 'Name')
      ])
  dt.LoadData(groups)
  return itty.Response(dt.ToJSonResponse(req_id=tqx.get('reqId', 0)),
                       content_type='application/json')

@itty.get('/articles')
def get_articles(request):
  query = request.GET.get('query', '')
  tq = parse_tq(request.GET.get('tq', ''))
  tqx = parse_tqx(request.GET.get('tqx', ''))

  with IDXR.cache_connection as cache:
    articles = cache.get_articles(subject=query, **tq)

  subjects = [a[2] for a in articles]
  LOG.debug(lcs(subjects))
  LOG.debug(commonprefix(subjects))

  dt = gviz_api.DataTable([
      ('post_date', 'datetime', 'Posted'),
      ('poster', 'string', 'Poster'),
      ('subject', 'string', 'Subject'),
      ('message_id', 'string', 'ID')
      ])
  dt_order = ['subject', 'post_date', 'poster', 'message_id']
  dt.LoadData(articles)
  return itty.Response(dt.ToJSonResponse(req_id=tqx.get('reqId', 0),
                                         columns_order=dt_order),
                       content_type='application/json')

@itty.get('/state')
def get_state(request):
  with IDXR.cache_connection as cache:
    status = {
      'jobs': IDXR.task_queue.qsize(),
      'articles': cache.article_count(),
      'groups': cache.group_count()
    }
  return itty.Response(json.dumps(status), content_type='application/json')

@itty.get('/jobs')
def get_jobs(request):
  return str(IDXR.task_queue.qsize())


@itty.get('/')
def index(request):
  return itty.serve_static_file(request, 'index.html')

@itty.get('/(?P<filename>.*)')
def index(request, filename):
  return itty.serve_static_file(request, filename)


@itty.post('/')
def actions(request):
  response = []

  if request.POST.get('reload_groups', False):
    IDXR.update_groups()
    response.append("Reloading groups...")

  fetch = request.POST.get('fetch', False)
  if fetch and len(fetch) == 2:
    LOG.debug(fetch)
    IDXR.get_last(fetch[0], int(fetch[1]))
    response.append("Fetching %s articles of %s..." % fetch)

  if request.POST.get('fetch_articles', False):
    IDXR.update_watched()
    response.append( "Fetching articles of watched groups...")

  if 'watch' in request.POST:
    groups = request.POST['watch']
    if not isinstance(groups, list): groups = [ groups ]
    with IDXR.cache_connection as cache:
      for group in groups:
        cache.set_watched(group, True)
        response.append( "Watching %s" % group)

  if 'unwatch' in request.POST:
    groups = request.POST['unwatch']
    if not isinstance(groups, list): groups = [ groups ]
    with IDXR.cache_connection as cache:
      for group in groups:
        cache.set_watched(group, False)
        response.append( "Not watching %s" % group)
  return itty.Response('\n'.join(response), content_type='text/plain')


if __name__ == '__main__':
  IDXR = Indexer()
  host = IDXR.config.get('server', 'host')
  port = IDXR.config.getint('server', 'port')
  itty.run_itty(host=host, port=port)
