

ROLE_ENDPOINT = 1
ROLE_CACHE = 2


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

    def sync(self):
        pass
