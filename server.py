import BaseHTTPServer
import logging
import os
import sqlite3

logging.basicConfig(format="%(levelname)s (%(threadName)s) %(filename)s:%(lineno)d %(message)s")
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)

def header_index():
    db = sqlite3.connect('cache.db')
    db_cursor = db.cursor()
    db_cursor.execute('select * from articles limit 100')
    rows = db_cursor.fetchall()
    return "headers"

BASE_PAGE = '<!DOCTYPE>\n<html><head>\n\t<title>\'dex</title>\n</head><body>%s</body></html>'
class IndexerView(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(BASE_PAGE % header_index())
        return
    
if __name__ == "__main__":
    try:
        httpd = BaseHTTPServer.HTTPServer(
            (os.environ['IP'], int(os.environ['PORT'])),
            IndexerView)
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.socket.close()