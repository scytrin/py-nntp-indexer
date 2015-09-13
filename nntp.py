#!/usr/bin/python

import itertools
import logging
import nntplib
import socket
import ssl
import threading


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


class Factory(object):
  def __init__(self, config):
    self.host = config['host']
    self.port = config['port']
    self.user = config.get('username')
    self.password = config.get('password')
    self.readermode = True
    self.usenetrc = True
    self.is_ssl = config.get('ssl', False)
    self.xover_span = config.get('xover_span', 100)
    self.connections = config.get('connections', 1)

    self._connections_semaphore = threading.BoundedSemaphore(self.connections)

  @property
  def connection(self):
    return self._Connection(
      self.host, self.port, self.user, self.password,
      readermode=True, usenetrc=True,
      is_ssl=self.is_ssl, xover_span=self.xover_span)

  class _Connection(nntplib.NNTP):
    """Intended to be used with the with statement!"""
    def __init__(self, host, port=nntplib.NNTP_PORT, user=None, password=None,
                 readermode=None, usenetrc=True, is_ssl=False, xover_span=None,
                 semaphore=None):
      self.host = host
      self.port = port
      self.user = user
      self.password = password
      self.xover_span_width = int(xover_span) or 100
      self.debugging = 0
      self.readermode = readermode
      self.usenetrc = usenetrc
      self.semaphore = semaphore or threading.BoundedSemaphore()

      self.is_ssl = is_ssl
      self.sock = None
      self.file = None
      self.welcome = None

    def connect(self, user=None, password=None):
      user = user or self.user
      password = password or self.password

      self.sock = socket.create_connection((self.host, self.port))
      if self.is_ssl:
        self.sock = ssl.wrap_socket(self.sock)
      self.file = self.sock.makefile('rb')
      self.welcome = self.getresp()

      readermode_afterauth = 0
      if self.readermode:
          try:
              self.welcome = self.shortcmd('mode reader')
          except nntplib.NNTPPermanentError:
              # error 500, probably 'not implemented'
              pass
          except nntplib.NNTPTemporaryError, e:
              if user and e.response[:3] == '480':
                  # Need authorization before 'mode reader'
                  readermode_afterauth = 1
              else:
                  raise
      # If no login/password was specified, try to get them from ~/.netrc
      # Presume that if .netc has an entry, NNRP authentication is required.
      try:
          if self.usenetrc and not user:
              import netrc
              credentials = netrc.netrc()
              auth = credentials.authenticators(self.host)
              if auth:
                  user = auth[0]
                  password = auth[2]
      except IOError:
          pass
      # Perform NNRP authentication if needed.
      if user:
          resp = self.shortcmd('authinfo user '+user)
          if resp[:3] == '381':
              if not password:
                  raise nntplib.NNTPReplyError(resp)
              else:
                  resp = self.shortcmd(
                          'authinfo pass '+password)
                  if resp[:3] != '281':
                      raise nntplib.NNTPPermanentError(resp)
          if readermode_afterauth:
              try:
                  self.welcome = self.shortcmd('mode reader')
              except nntplib.NNTPPermanentError:
                  # error 500, probably 'not implemented'
                  pass

    def __enter__(self):
      self.semaphore.acquire()
      self.connect()
      return self

    def __exit__(self, exec_type, exec_val, exec_traceback):
      self.quit()
      self.semaphore.release()
      return False

    def _range_spans(x, y, span):
      first = min(int(x), int(y))
      last = max(int(x), int(y))
      for start in xrange(first, last, span):
        yield str(start), str(min(last, start + span - 1))

    def xover_span(self, group_name, first, last):
      first = int(first)
      last = int(last)
      self.group(group_name)
      for page in itertools.count(1, 150):
        start = min(last, first + page * self.xover_span_width)
        end = min(last, first + (page + 1) * self.xover_span_width)
        LOG.info('Fetching %i to %i (%i) from %s', start, end, page, group_name)
        response = self.xover(str(start), str(end))
        for article in response[1]:
          yield article
