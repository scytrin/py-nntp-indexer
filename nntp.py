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

DEFAULT_XOVER_SPAN = 100
DEFAULT_NNTP_PORT = 119
DEFAULT_CONNECTIONS = 1

LONGRESP = ['100', '215', '220', '221', '222', '224', '230', '231', '282']
CRLF = '\r\n'

class NNTP(nntplib.NNTP):
  @classmethod
  def FromConfig(cls, config):
    return cls(
      config['host'], config.get('port', DEFAULT_NNTP_PORT),
      config.get('username'), config.get('password'),
      True, True, config.get('ssl', False),
      config.get('xover_span', DEFAULT_XOVER_SPAN),
      config.get('connections', DEFAULT_CONNECTIONS))

  def __init__(self, host, port, user=None, password=None,
               readermode=None, usenetrc=True, is_ssl=False, xover_span=None,
               max_connections=None):
    self.host = host
    self.port = port
    self.user = user
    self.password = password
    self.readermode = readermode
    self.usenetrc = usenetrc
    self.is_ssl = is_ssl
    self.xover_span_width = int(xover_span or DEFAULT_XOVER_SPAN)
    self._connections_semaphore = threading.BoundedSemaphore(max_connections or DEFAULT_CONNECTIONS)
    self._authentication = lambda: self.__authenticate(user, password)
    self.__thread_local = threading.local()
    self.__thread_local.sock = None
    self.__thread_local.file = None
    self.__thread_local.welcome = None

    self.debugging = 0

  def __str__(self):
    return '<NNTP %s %i>' % (self.host, self.port)

  @property
  def sock(self):
    if getattr(self.__thread_local, 'sock', None) is None:
      LOG.debug('opening socket to %s %i', self.host, self.port)
      sock = socket.create_connection((self.host, self.port))
      if self.is_ssl:
        sock = ssl.wrap_socket(sock)
      self.__thread_local.sock = sock
    return self.__thread_local.sock

  @property
  def file(self):
    if getattr(self.__thread_local, 'file', None) is None:
      LOG.debug('opening file for %s %i : %s', self.host, self.port, self.sock)
      self.__thread_local.file = self.sock.makefile('rb')
    return self.__thread_local.file

  @property
  def welcome(self):
    return self.__thread_local.welcome

  def __enter__(self):
    LOG.info('connecting to %s:%i', self.host, self.port)
    self._connections_semaphore.acquire(True)

    # Commiting some magic here
    self.__thread_local.welcome = self.getresp()

    self._authentication()
    return self

  def __exit__(self, exec_type, exec_val, exec_traceback):
    self.file.close()
    self.sock.close()
    self.__thread_local.file = None
    self.__thread_local.sock = None

    self._connections_semaphore.release()
    return False

  def __authenticate(self, user, password):
    readermode_afterauth = 0
    if self.readermode:
        try:
            self.__thread_local.welcome = self.shortcmd('mode reader')
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
                self.__thread_local.welcome = self.shortcmd('mode reader')
            except nntplib.NNTPPermanentError:
                # error 500, probably 'not implemented'
                pass

  def xover_span(self, group_name, start, end):
    start, end = int(start), int(end)
    top = max(start, end)
    starts = xrange(start, end, self.xover_span_width)
    if start > end:
      starts = reversed(starts)

    LOG.debug('Grabbing articles [%i-%i] from %s', start, end, group_name)
    self.group(group_name)
    for low in starts:
      high = min(top, low + self.xover_span_width - 1)
      LOG.debug('Fetching [%i-%i] from %s', low, high, group_name)
      response = self.xover(str(low), str(high))
      for article in response[1]:
        yield article
