from publisher import app
from gevent.wsgi import WSGIServer
from gevent.threadpool import ThreadPool

http_server = WSGIServer(('', 8443), app)
http_server.serve_forever()

