#!/usr/bin/python2.7

import indexer

def main():
  app = indexer.Indexer('defaults.cfg')
  app.build_group_list()
  with app.cache_connection as cache:
    cache.set_watched('alt.binaries.tv', True)
    cache.set_watched('alt.binaries.hdtv', True)
  app.update_watched()

if __name__ == "__main__":
  main()
