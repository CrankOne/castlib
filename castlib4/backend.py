import os, zlib, pwd, grp, re

class DetailedList(object):
    """
    Mixin for recursive traversal. Might be included in backends implementing
    standard "interface", which does not provide detailed retrieval of the
    filesystem attributes at once.
    """
    def ls_detailed( self, path, recursive=False
                   , filePropertiesExclude=set({})
                   , dirPropertiesExclude=set({})
                   , omitEmptyDirs=False
                   , pattern='*' ):
        """
        Arguments:
            recursive -- contols whether or not to traverse sub-directories
        filePropertiesExclude -- set (or list) containing list of properties that
            have to be excluded from resulting dictionary
        dirPropertiesExclude -- set (or list) containing list of properties that
            have to be excluded from resulting dictionary
        omitEmptyDirs -- controls, whether to omit the empty directories from
            result
        regex -- regular expression that, if given, shall be applied against full
            path to approve inclusion of the file.

        NOTE: for globbing pattern that is easy to use, consider usage of
        fnmatch.translate() function: https://docs.python.org/3/library/fnmatch.html#fnmatch.translate
        """
        get_ = lambda mtd, p: getattr(self, mtd)(p) if hasattr(self, mtd) else None
        entries = {}
        files, dirs = self.ls(path)
        for f in files:
            fp = os.path.join(path, f)
            if '*' != pattern and not re.match(pattern, fp):
                continue
            entries[f] = { k: get_(p, fp) for (k, p) in [
                ('size',    'file_size'),
                ('adler32', 'adler32' ),
                ('mtime', 'get_mtime' ),
                ('owner', 'get_owner' ),
                ('permissions', 'get_permissions'),
                # ...
                ] if k not in filePropertiesExclude }
        for d in dirs:
            dp = os.path.join(path, d)
            ec = { k: get_(p, dp) for (k, p) in [
                ('mtime', 'get_mtime' ),
                ('owner', 'get_owner' ),
                ('permissions', 'get_permissions'),
                # ...
                ] if k not in dirPropertiesExclude }
            de = self.ls_detailed( dp
                                 , recursive=True
                                 , filePropertiesExclude=filePropertiesExclude
                                 , dirPropertiesExclude=dirPropertiesExclude
                                 , omitEmptyDirs=omitEmptyDirs
                                 , pattern=pattern ) if recursive else None
            if de is None \
            or ( not de \
                 and omitEmptyDirs ):
                continue
            ec['content'] = de
            entries[d] = ec
        return entries



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

