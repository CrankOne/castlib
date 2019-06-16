

ROLE_ENDPOINT = 1
ROLE_CACHE = 2


class BackendInterface(object):
    """
    An interface to particular backend.
    """
    def get_entries_at(self, path):
        """
        Returns the list of entries by given path (or path-like identifier).
        """
        pass

# ...
castorMappingAttributes = {
    'name' : 'name',
    'size' : lambda gs : int(gs['size']),
    'checksums' : {
        'adler32' : 'adler32'
    }
}
# ...

class Storage(object):
    """
    Storage instances
    """

    def __init__(self, name, backend=None):
        self._name = name
        if type(backend) is str:
            self._backend = instantiate_backend(backend)
        else:
            self._backend = backend
        pass

    def sync_state_model(self):
        """
        Retrieves the state of the storage.
        """
        pass
