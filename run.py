#!/usr/bin/python2.7

import ConfigParser
import cache, indexer, server

config = ConfigParser.SafeConfigParser()
config.readfp(open('defaults.cfg'))
indexer = indexer.Indexer(config)
