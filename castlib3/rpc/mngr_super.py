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

import xmlrpclib

class WorkerManager_Supervisord(object):
    def __init__( self
                , nodeName
                , portNo
                , username=None
                , password=None):
        self.nodeName = nodeName
        self.portNo = portNo
        # xmlrpclib.Server( 'http://physicist:stopjanedoe@pcdmdb01.cern.ch:8089' )
        connectionString = ''
        if username:
            connectionString = 'http://%s:%s@%s:%d'%( username, password, nodeName, portNo )
        else:
            connectionString = 'http://%s:%d'%( nodeName, portNo )
        self.server = xmlrpclib.Server( connectionString )

    def status(self):
        """
        Returns a tuple: boolean value, denoting whether the manager is
        available, and details dict in form:
            {'statecode': 1, 'statename': 'RUNNING'}
        Possible values are:
            2 FATAL (Supervisor has experienced a serious error.)
            1 RUNNING (Supervisor is working normally, only considered as available.)
            0 RESTARTING (Supervisor is in the process of restarting.)
            -1 SHUTDOWN (Supervisor is in the process of shutting down.)
        """
        try:
            ret = self.server.supervisor.getState()
        except Exception as e:
            return False, {}
        else:
            return ret['statecode'] == 1, ret

    def get_API_version(self):
        """
        Returns a standard version string: 'V.V.V'.
        """
        return self.server.supervisorgetSupervisorVersion()

    def worker_status(self, workerID):
        """
        Returns detailed info of running process. See ref at:
        http://supervisord.org/api.html#supervisor.rpcinterface.SupervisorNamespaceRPCInterface.getProcessInfo
        S.a.: getAllProcessInfo()
        """
        return self.server.supervisor.getProcessInfo(workerID)

    def start_worker(self, workerID):
        return self.server.supervisor.startProcess(workerID)

    def stop_worker(self, workerID):
        return self.server.supervisor.stopProcess(workerID)

