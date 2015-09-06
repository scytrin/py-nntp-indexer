#!/usr/bin/python

import logging
import nntplib
import socket
import ssl


logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


class NNTP(nntplib.NNTP):
  @classmethod
  def Connect(cls, config):
    return cls(
      config['host'], config['port'], config['username'], config['password'],
      True, True, config.get('ssl', False), config.get('xover_span', 100))

  def __init__(self, host, port=nntplib.NNTP_PORT, user=None, password=None,
               readermode=None, usenetrc=True, is_ssl=False, xover_span=None):
    self.sock = socket.create_connection((host, port))
    if is_ssl:
      self.sock = ssl.wrap_socket(self.sock)
    self.host = host
    self.port = port
    self.span = int(xover_span) or 100
    self.file = self.sock.makefile('rb')
    self.debugging = 0
    self.welcome = self.getresp()

    readermode_afterauth = 0
    if readermode:
        try:
            self.welcome = self.shortcmd('mode reader')
        except nntplib.NNTPPermanentError:
            pass
        except nntplib.NNTPTemporaryError, e:
            if user and e.response[:3] == '480':
                readermode_afterauth = 1
            else:
                raise
    try:
        if usenetrc and not user:
            import netrc
            credentials = netrc.netrc()
            auth = credentials.authenticators(host)
            if auth:
                user = auth[0]
                password = auth[2]
    except IOError:
        pass
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
                pass

  def __enter__(self):
    return self

  def __exit__(self, exec_type, exec_val, exec_traceback):
    self.quit()
    return False

  def xover_spans(self, first, last):
    for start in xrange(int(first), int(last), self.span):
      end = start + (self.span - 1)
      response = self.xover(str(start), str(end))
      for article in response[1]:
        yield article
