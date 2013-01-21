import logging
import nntplib

logging.basicConfig(format="%(levelname)s (%(processName)s:%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

class Worker(nntplib.NNTP):
  def __init__(self, semaphore, *args, **kw):
    self.semaphore = semaphore
    self.semaphore.acquire()
    nntplib.NNTP.__init__(self, *args, **kw)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.quit()

  def quit(self):
    nntplib.NNTP.quit(self)
    self.semaphore.release()
