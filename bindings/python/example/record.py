from tonbo import Record, Column, DataType


@Record
class User:
    id = Column(DataType.Int64, name="id", primary_key=True)
    age = Column(datatype=DataType.UInt8, name="age", nullable=True)
    # nullable is `False` by default
    name = Column(DataType.String, name="name")
    height = Column(DataType.Int16, name="height")
    enabled = Column(DataType.Boolean, name="enabled")
    email = Column(DataType.String, name="email", nullable=True)
    data = Column(DataType.Bytes, name="data", nullable=True)

