#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

import cStringIO
import Image
import urllib2
import gflags
import re
import os
import sys
import web
import traceback

from pygaga.helpers.logger import log_init

from guang_crawler.view import app

FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('server', True, "is run as standard server")
gflags.DEFINE_boolean('fcgi', False, "is run as fcgi server")
gflags.DEFINE_string('args', "0.0.0.0:8765", "wsgi args")
gflags.DEFINE_string('crawl_path', "/space/crawler/image_crawler/static", "image path")

if __name__ == "__main__":
    try:
        argv = FLAGS(sys.argv)[1:]  # parse flags
    except gflags.FlagsError, e:
        print '%s\\nUsage: %s ARGS\\n%s' % (e, sys.argv[0], FLAGS)
        sys.exit(1)

    log_init()

    newargv = []
    newargv.append(sys.argv[0])
    newargv.append(FLAGS.args)
    sys.argv = newargv
    if FLAGS.fcgi:
        web.wsgi.runwsgi = lambda func, addr=None: web.wsgi.runfcgi(func, addr)
    app.run()

