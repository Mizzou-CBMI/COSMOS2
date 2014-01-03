import sqlalchemy.types as types

class Enum34(types.TypeDecorator):
    """
    Enum compatible with enum34 package
    """

    def __init__(self, enum_instance):
        self.enum_instance = enum_instance
        impl = types.Enum(['a','b'])

    def process_bind_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        return value

    def copy(self):
            return Enum34(self.enum_instance)

