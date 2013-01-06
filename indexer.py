import nntplib
import cache

class Indexer:
    def __init__(config):
        self.nntp = nntplib.NNTP(
            config.get('indexer', 'host'),
            config.get('indexer', 'port'),
            config.get('indexer', 'username'),
            config.get('indexer', 'password')
            )
    