import os
import sys
import gflags

from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.python import log

from zope.interface import implements
from scrivener import ScribeServerService
from scrivener.handlers import TwistedLogHandler
from scrivener.interfaces import ILogHandler

from pygaga.helpers.logger import log_init

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('port', 1234, "bind port")

class MyTwistedLogHandler(object):
    implements(ILogHandler)

    def log(self, category, message):
        if category == 'click':
            pass
        elif category == 'middle':
            pass
        elif category == 'action':
            pass
        log.msg(">>c " + category + " m " + message.strip(), system=category)
        #print "c", category, "m", message

def main():
    service = ScribeServerService(
        TCP4ServerEndpoint(reactor, FLAGS.port),
        MyTwistedLogHandler())
    service.startService()

if __name__ == "__main__":
    #log.startLogging(sys.stdout)
    observer = log.PythonLoggingObserver()
    observer.start()
    log_init()
    reactor.callWhenRunning(main)
    reactor.run()
