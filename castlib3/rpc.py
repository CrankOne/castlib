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
Remote pipeline procedure call utility functions: listening socket and
transmitting the progress information.
"""

import sys, socket, json, yaml, pickle, struct, threading
from castlib3.logs import gLogger, mh
from collections import OrderedDict
from progress.bar import Bar
from datetime import datetime
from time import time
from dateutil.relativedelta import relativedelta

def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)

def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)

def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = ''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def recieve_stages( srvAddr, portNo ):
    """
    First approach implementation --- just receives the first stages list and
    closes the connection.
    """
    sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
    sock.bind(( srvAddr, portNo ))
    gLogger.info( 'Listening connections on %s:%d...'%(srvAddr, portNo) )
    sock.listen(1)
    conn, cliAddr = sock.accept()
    try:
        data = recv_msg( conn )
        dataType = data[:4]
        data = data[4:]
        if dataType == 'json':
            send_msg( conn, 'json{"status":"ok"}' )
            return json.loads(data)
        elif dataType == 'yaml':
            send_msg( conn, 'yaml{"status":"ok"}' )
            return yaml.load(data)
        elif dataType == 'pckl':
            send_msg( conn, 'pckl' + pickle.dumps({'status':'ok'}) )
            return pickle.loads(data)
        else:
            send_msg( conn, 'json{"status":"bad data type"}' )
            raise TypeError("Unsupported data format: `%s'"%dataType)
    finally:
        conn.close()

class Reporter( threading.Thread ):
    def __init__(self, *args, **kwargs):
        srvAddr = kwargs.pop('srvAddr')
        portNo = kwargs.pop('portNo')
        super(Reporter, self).__init__(*args, **kwargs)
        self.sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        self.sock.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(( srvAddr, portNo ))
        self.stagesCV = threading.Condition()
        self.stages = {'stages':{}}
        self.progress = None
        self.alive = threading.Event()

    def start(self, *args, **kwargs):
        self.alive.set()
        super(Reporter, self).start( *args, **kwargs )

    def run(self):
        while True and self.alive.is_set():
            self.sock.listen(1)
            conn, cliAddr = self.sock.accept()
            try:
                data = recv_msg( conn )
                if len(data) < 4:
                    send_msg( conn, 'json{"status":"Bad request."}' )
                else:
                    dataType = data[:4]
                    data = data[4:]
                    if dataType == 'json':
                        send_msg( conn, 'json' + json.dumps(self.treat_request( json.loads(data) )) )
                    elif dataType == 'yaml':
                        send_msg( conn, 'yaml' + yaml.dump(self.treat_request( json.loads(data) )) )
                    elif dataType == 'pckl':
                        send_msg( conn, 'pckl' + pickle.dumps(self.treat_request( pickle.loads(data) )) )
                    else:
                        send_msg( conn, 'json{"status":"Unrecognized request data type."}' )
            finally:
                conn.close()

    def treat_request( self, data ):
        resp = { 'status' : 'ok' }
        if 'stages' in data.keys():
            if self.stagesCV.acquire(blocking=0):
                self.stages = {'stages':data['stages']}
                self.stagesCV.notify_all()
                self.stagesCV.release()
                resp['status'] = 'Accepted.'
                return resp
            else:
                resp['status'] = 'Busy.'
        if 'progress' in data.keys():
            if self.progress and issubclass(self.progress.__class__, Bar):
                resp['progress'] = (self.progress.index, self.progress.max)
            else:
                resp['progress'] = (1, 1)
        if 'progress_message' in data.keys():
            if self.progress and issubclass(self.progress.__class__, Bar) and self.progress.message:
                resp['progress_message'] = self.progress.message
            else:
                resp['progress_message'] = 'unknown activity'
        if 'latest_msgs' in data.keys():
            resp['latest_msgs'] = mh.get_records()
        return resp


def human_readable_time( deltaSecs ):
    delta = relativedelta(seconds=deltaSecs)
    attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
    return ', '.join( ['%d %s'%( getattr(delta, attr), getattr(delta, attr) > 1 and attr or attr[:-1] )
                        for attr in attrs if getattr(delta, attr)] )

class ReportingBar(Bar):
    def __init__(self, *args, **kwargs):
        self.reporter = kwargs.pop('reporter', None)
        super(ReportingBar, self).__init__(*args, **kwargs)
        if self.reporter:
            self.reporter.progress = self

    @property
    def prec_hr_time(self):
        dlt = time() - self.start_ts
        #return human_readable_time( self.index*self.remaining/dlt ) if dlt > 0 else '--'
        return human_readable_time(self.remaining*(dlt/float(self.index))) if dlt > 0 else '--'

    def finish(self, *args, **kwargs):
        super(ReportingBar, self).finish(*args, **kwargs)
        if self.reporter:
            self.reporter.progress = None


