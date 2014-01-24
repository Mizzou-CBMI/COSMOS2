import sqlalchemy.types as types
from sqlalchemy.ext.mutable import Mutable

class Enum34_ColumnType(types.TypeDecorator):
    """
    Enum compatible with enum34 package
    """

    impl = types.Enum

    def __init__(self, enum_class):
        self.enum_class = enum_class
        return types.TypeDecorator.__init__(self, *enum_class._member_names_)

    def process_bind_param(self, value, dialect):
        assert isinstance(value, self.enum_class), "'%s' must be of type %s" % (value, self.enum_class)
        return value.name

    def process_result_value(self, value, dialect):
        return getattr(self.enum_class, value)

    def copy(self):
        return Enum34_ColumnType(self.enum_class)

class ListOfStrings(types.TypeDecorator):
    """
    Enum compatible with enum34 package
    """
    impl = types.String

    def __init__(self):
        return types.TypeDecorator.__init__(self, '')

    def process_bind_param(self, value, dialect):
        assert isinstance(value, list), '%s must be a list' % value
        return ', '.join(value) if value else None

    def process_result_value(self, value, dialect):
        return value.split(', ') if value else []

def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance, True

from sqlalchemy.types import TypeDecorator, VARCHAR
import json

class JSONEncodedDict(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

class MutableDict(Mutable, dict):

    @classmethod
    def coerce(cls, key, value):
        if not isinstance(value, MutableDict):
            if isinstance(value, dict):
                return MutableDict(value)
            return Mutable.coerce(key, value)
        else:
            return value

    def __delitem(self, key):
        dict.__delitem__(self, key)
        self.changed()

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self.changed()

    def __getstate__(self):
        return dict(self)

    def __setstate__(self, state):
        self.update(self)