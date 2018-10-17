from ..field import Field
from ..tools import validation
from ..enum import FieldType


class Cell(Field):
    def validate(self, parent=None):
        errors = []

        if not self.name:
            errors.append(validation.name_missing(self.name, parent))
        if not self.type or self.type == FieldType.Nothing:
            errors.append(validation.type_missing(self.name, parent))

        return errors

    def to_dict(self):
        result = super().to_dict()

        if result:
            result.pop('fdn', None)

        return result

    def from_dict(self, cell_dict):
        return super().from_dict(cell_dict)