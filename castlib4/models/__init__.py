__all__ = [
        'filesystem'
    ]

import sqlamp

# The models declared using SQLAlchemy declarative base requires common base
# class inststance declared.

DeclBase = None

try:
    from sVresources.db.instance import gBase as DeclBase_
    DeclBase = DeclBase_
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base
    DeclBase = declarative_base(name='castlib4DeclarativeBase',
                                metaclass=sqlamp.DeclarativeMeta)
assert(DeclBase)
import sys
setattr( sys.modules[__name__], 'DeclBase', DeclBase )
