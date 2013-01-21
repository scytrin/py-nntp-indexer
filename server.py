import json
import logging
from urlparse import parse_qs, urlparse
import wsgiref
from wsgiref.simple_server import make_server
from wsgiref.util import FileWrapper

from indexer import Indexer

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

class IndexServer:
  @classmethod
  def start(self, config_file='defaults.cfg'):
    indexer = Indexer(config_file)
    host = indexer.config.get('server', 'host')
    port = indexer.config.getint('server', 'port')
    def inject_indexer(environ, start_response):
      environ['indexer'] = indexer
      return self(environ, start_response)
    make_server(host, port, inject_indexer).serve_forever()

  def __init__(self, environ, start_response):
    self.environ = environ
    self.start = start_response
    self.path_info = urlparse(self['PATH_INFO'])
    self.query = parse_qs(self['QUERY_STRING'])
    self._post_params = None

  def __getitem__(self, key):
    return self.environ[key]

  @property
  def indexer(self):
    return self['indexer']

  @property
  def post_params(self):
    if not self._post_params:
      self._post_params = {}
      if self.environ['REQUEST_METHOD'].upper() == 'POST':
        content_type = self.environ.get('CONTENT_TYPE', 'application/x-www-form-urlencoded')
        if (content_type.startswith('application/x-www-form-urlencoded')
            or content_type.startswith('multipart/form-data')):
          try:
            request_body_size = int(self.environ.get('CONTENT_LENGTH', 0))
          except (ValueError):
            request_body_size = 0
          self._post_params = parse_qs(self['wsgi.input'].read(request_body_size))
          LOG.debug(self._post_params)
    return self._post_params

  def params(self, key, *default, **kw):
    which = kw.get('which', 0)
    transform = kw.get('transform', lambda x: x)
    params = map(transform, self.query.get(key, default))
    if which is None:
      return params
    elif len(params) > which:
      return params[which]
    elif len(default) > which:
      return default[which]
    else:
      return None

  def __iter__(self):
    if self['REQUEST_METHOD'] == 'POST' and self.path_info.path == '/':
      return self.process_post()
    elif self['REQUEST_METHOD'] == 'GET':
      func_name = self.path_info.path.lstrip('/').split('/')[0]
      if func_name == '':
        func_name = 'index'
      return getattr(self, func_name, self.err_404)()
    else:
      return err_405()

  def process_post(self):
    self.start('200 OK', [('Content-type', 'text/plain')])
    if self.post_params.get('reload_groups', False):
      self.indexer.update_groups()
      yield "Reloading groups..."
    if self.post_params.get('fetch_articles', False):
      self.indexer.update_watched()
      yield "Fetching articles of watched groups..."
    if 'watch' in self.post_params:
      with self.indexer.cache_connection as cache:
        for group in self.post_params['watch']:
          cache.set_watched(group, True)
          yield "Watching %s" % group
    if 'unwatch' in self.post_params:
      with self.indexer.cache_connection as cache:
        for group in self.post_params['unwatch']:
          cache.set_watched(group, False)
          yield "Not watching %s" % group

  def err_404(self):
    self.start('404 Not Found', [('Content-Type', 'test/plain')])
    yield 'nothing found!!!'

  def err_405(self):
    self.start('405 Method Not Allowed', [('Content-Type', 'test/plain')])
    yield 'Not allowed!'

  def index(self):
    self.start('200 OK', [('Content-type', 'text/html')])
    return FileWrapper(open('index.html'))

  def find_items(self, func):
    query = self.params('q', '', which=-1)
    limit = self.params('l', 100, transform=int)
    offset = self.params('o', 0, transform=int)
    LOG.debug((query, limit, offset))
    return func(query, limit, offset)

  def groups(self):
    with self.indexer.cache_connection as cache:
      groups = self.find_items(cache.get_groups)
    LOG.debug(groups)
    self.start('200 OK', [('Content-type', 'application/json')])
    yield json.dumps(groups)

  def articles(self):
    with self.indexer.cache_connection as cache:
      articles = self.find_items(cache.get_articles)
    self.start('200 OK', [('Content-type', 'application/json')])
    yield json.dumps(articles)


if __name__ == '__main__':
  IndexServer.start()
