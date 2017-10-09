# -*- coding: utf-8 -*-
# Copyright (c) 2016 Renat R. Dusaev <crank@qcrypt.org>
# Author: Renat R. Dusaev <crank@qcrypt.org>
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from __future__ import print_function

"""
File containing logging configuration for castlib2.
"""

import logging, os, threading
from collections import deque

class MemorizingHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        n = kwargs.pop('nMaxMessages', 10)
        self.memo = deque( n*[None], n )
        self.lock = threading.RLock()
        logging.Handler.__init__(self)

    def emit(self, record):
        msg = self.format( record )
        with self.lock:
            self.memo.appendleft( msg )

    def get_records(self):
        rs = []
        with self.lock:
            rs = list([ r for r in self.memo ])
        return filter( lambda r: r, rs )

# create logger
gLogger = logging.getLogger('CastLib3')
gLogger.setLevel( logging.DEBUG )

# create console handler and set level to debug
sh = logging.StreamHandler()
dbgOn = True if 'DEBUG' in os.environ.keys() and int(os.environ['DEBUG']) else False
if dbgOn:
    sh.setLevel( logging.DEBUG )
else:
    sh.setLevel( logging.INFO )
formatter = logging.Formatter('%(name)s/%(levelname)s: %(message)s')
sh.setFormatter(formatter)

ch = logging.FileHandler('/tmp/castlib.log' )
ch.setLevel( logging.INFO )

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# memorizing handler
mh = MemorizingHandler()
mh.setLevel( logging.DEBUG if dbgOn else logging.INFO )

gLogger.addHandler(ch)
gLogger.addHandler(mh)
gLogger.info('*** New castlib2 session started ***')
gLogger.addHandler(sh)


# XXX:
#gLogger.debug('debug message')
#gLogger.info('info message')
#gLogger.warn('warn message')
#gLogger.error('error message')
#gLogger.critical('critical message')

