"""
Dir contains a set of functions utilizing the castlib2 functionality for
synchronization database with local and (or) original entires, and remote
CASTOR directories.

Can be used for end-user script composition.
"""

__all__ = [ 'indexLocalDir'
          , 'send'
          , 'select'
          , 'sync'
          , 'na64-web-page'  # XXX, tmp
    ]

from . import *
