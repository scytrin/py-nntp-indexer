#!/usr/bin/python2.7

import ConfigParser
import logging
import multiprocessing
from multiprocessing import Pool, Process
from nntplib import NNTP
import signal
import StringIO
import sqlite3
from Queue import Queue
import thread
import threading
import time

from indexer import Indexer

logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def main():
  indexer = Indexer('defaults.cfg')
  #indexer.build_group_list()
  indexer.update_watched()

if __name__ == "__main__":
  main()
