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

import os, zlib, pwd, grp
from .mixins import DetailedList

class LocalBackend(DetailedList):
    """
    This class implements a local "back-end" routines.

    One may consider this implementation as a kind of explicit contract or
    an "implemented interface", the boilerplate claiming of how all other
    back-ends must behave.
    """

    def __init__(self, a32blockSize=65536):
        self.a32blockSize = a32blockSize

    def ls(self, path, pattern=None):
        """
        Returns two lists: file names and folder names, found by the given
        path. If path points to the file, empty lists will be returned. If path
        does not exist, the `FileNotFoundError' will be raised. If file instead
        of directory found by path, `NotADirectoryError' is thrown.
        """
        entries = [e for e in os.listdir(path) if not os.path.islink(os.path.join(path, e))]
        if pattern:
            entries = [e for e in entries if fnmatch.fnmatch(e, pattern)]
        files = [ f for f in entries if not os.path.isdir(os.path.join(path, f)) ]
        return files \
             , [ d for d in entries if d not in files ]

    def adler32(self, path, blocksize=None):
        """
        Returns Adler32 checksum as a hex string of length 8. Raise
        `IsADirectoryError' if given path points to a file and
        `FileNotFoundError' if file does not exist.
        """
        checksum = zlib.adler32(b"")
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(blocksize if blocksize is not None else self.a32blockSize), b""):
                checksum = zlib.adler32(block, checksum)
        return '%08x'%(checksum & 0xffffffff)

    def file_size(self, path):
        """
        Returns size of a file found by given path in bytes. Raises
        `FileNotFoundError' if file does not exist, `IsADirectoryError' if path
        points to a directory.
        """
        if os.path.isdir(path):
            raise IsADirectoryError(path)
        return os.path.getsize(path)

    def get_mtime(self, path):
        """
        Returns modification timestamp (as integer).
        """
        return int(os.path.getmtime(path))

    def get_owner(self, path):
        """
        Returns ('user', UID, 'group', GID) tuple.
        """
        si = os.stat(path)
        return { 'user'  : (pwd.getpwuid(si.st_uid)[0], si.st_uid)
               , 'group' : (grp.getgrgid(si.st_gid)[0], si.st_gid) }

    def get_permissions(self, path):
        """
        Returns numerical identifier denoting permissions of file or folder.
        """
        si = os.stat(path)
        return si.st_mode

    #
    # Modifications

    def set_mtime(self, path):
        raise NotImplementedError()  # TODO

    def mkdir(self, path):
        raise NotImplementedError()  # TODO

