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

import re
import time
import datetime

# 3 columns: the basename of the HSM name, the fileid in the used castor name
# server, and the current status of the found record
rxsCastorHSMFilename = "(/([^/\s]*))+/?"
rxsCastorFileID = "\d+@\w+"
rxsCastorQueryState = "STAGEIN|STAGED|STAGEOUT|CANBEMIGR|INVALID"
rxsQryResponse = r"^" + r"\s*(?P<hsmFilename>" + rxsCastorHSMFilename + ")" \
                + r"\s+(?P<fileID>" + rxsCastorFileID + ")" \
                + r"\s+(?P<status>" + rxsCastorQueryState + ")" \
                + r"$"

# groups: hsmFilename, fileID, status
rxStagerQueryResponse = re.compile( rxsQryResponse )

# groups: requestID
rxStagerGetRequestResponse = re.compile( '.*' )  # TODO

# groups: requestID
rxStagerPutRequestResponse = re.compile( '.*' )  # TODO

rxsNSLSFileClass    = r'^\s*(?P<fileClass>\S+)\s+'
rxsNSLSMode         = r'(?P<mode>[dDlmrwx\-st]+)\s+'
rxsNSLSDirNentries  = r'(?P<dirNEntries>\w+)\s+'
rxsNSLSOwnerNGroup  = r'(?P<ownerName>[\w-]+)\s+(?P<ownerGroupName>[\w-]+)\s+'
rxsNSLSFileSize     = r'(?P<fileSize>\d+)\s+'
rxsNSLSTimestamp    = r'(?P<modMonth>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(?P<modDate>\d{1,2})\s+((?P<modYear>\d{4})|(?P<modTime>\d{1,2}:\d{1,2}))'
rxsNSLSChecksum     = r'(?P<checksum>(:?\s+AD\s+(?P<adler32>[0-9a-fA-F]+)))?\s+'
# groups: todo
rxsNSLS = rxsNSLSFileClass      \
        + rxsNSLSMode           \
        + rxsNSLSDirNentries    \
        + rxsNSLSOwnerNGroup    \
        + rxsNSLSFileSize       \
        + rxsNSLSTimestamp      \
        + rxsNSLSChecksum       \
        + r'(?P<filename>.+)$'
rxNSLS = re.compile( rxsNSLS, re.M )

rxsStagerQueryError = r'(?:Error\s+(?P<errorCode>\d+))\/(?P<errorMessage>.+)'
rxStagerQueryError = re.compile( rxsStagerQueryError )

rxsStagerSubrequesFailure = r'(?:(?P<filename>\/[\w\.\/-]+)\s+' \
                            r'(?P<subrequestStatus>SUBREQUEST_(?P<subreqStatPart>\w+))(\s+' \
                            r'((?P<error>(?P<errorCode>\d+)\s+' \
                            r'(?P<errorMessage>.+))))?)'
rxsStagerRequestID = r'(:?Stager request ID: (?P<StagerRequestUUID>([0-9a-fA-F\-]+)+))'
rxStagerSubrequesFailure = re.compile(
    '^' + rxsStagerSubrequesFailure + '|' + rxsStagerRequestID + '$',
    re.M )

rfstatTimeReXs = \
    "^Last\s(?P<type>access|modify|stat\.\smod\.)\s+\:\s*(?P<timeString>.*)\s*$"

def obtain_rfstat_timestamps( rfstatOut ):
    """
    Obtains access/modification/metadata modification timestamps from
    rfstat util outout.
    """
    res = {}
    rfstatTimeReX = re.compile( rfstatTimeReXs )
    for l in rfstatOut.splitlines():
        m = rfstatTimeReX.match(l)
        if m:
            typeDT = None
            valDT = None
            for k, v in m.groupdict().iteritems():
                if 'type' == k:
                    if 'access' == v:
                        typeDT = 'accTimestamp'
                    elif 'modify' == v:
                        typeDT = 'modTimestamp'
                    elif 'stat. mod.' == v:
                        typeDT = 'statModTimestamp'
                else:
                    valDT = int(time.mktime(datetime.datetime.strptime(v, "%a %b %d %H:%M:%S %Y").timetuple()))
            res[typeDT] = valDT
    return res


