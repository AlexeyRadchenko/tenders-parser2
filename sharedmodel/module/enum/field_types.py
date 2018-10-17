from enum import Enum

class FieldType(Enum):
    Nothing = 0
    Integer = 1
    String = 2
    DateTime = 3
    Date = 4
    Price = 5
    Array = 6
    Bool = 7
    Object = 8