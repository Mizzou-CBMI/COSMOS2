import sqlalchemy.types as types

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

def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance, True