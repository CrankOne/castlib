import sys, pkgutil

class _BackendFactory(object):
    def __init__(self):
        self._ctrs = {}

    def register_backend(self, name, ctr):
        assert( name not in self._ctrs.keys() )
        self._ctrs[name] = ctr

    def get_backend( self, name ):
        return self._ctrs[name]


setattr( sys.modules[__name__], 'registry', _BackendFactory() )

__all__ = ['local' ]
