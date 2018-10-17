# from module.enum.modifications import Modification
from .enum import FieldType, Modification
from .tools import validation


class Field(object):
    def __init__(self, name=None, type=FieldType.Nothing, value=None, displayName=None, modifications=None, changes=None):
        self.set_properties(name, type, value, displayName,
                            modifications, changes)

    def set_properties(self, name=None, type=FieldType.Nothing, value=None, displayName=None, modifications=None, changes=None):
        self.name = name
        self.type = type
        self.value = value
        self.displayName = displayName
        self.modifications = modifications if modifications else []
        self.changes = changes if changes else dict()

        return self

    def compare(self, other, other_date, self_date):
        if other.value == self.value:
            return
        if other_date not in self.changes:
            self.changes[other_date] = other.value
        if self_date not in self.changes:
            self.changes[self_date] = self.value

    def validate(self, parent=None):
        errors = []

        if not self.name:
            errors.append(validation.name_missing(self.name, parent))
        if not self.displayName:
            errors.append(validation.display_name_missing(self.name, parent))
        if not self.type or self.type == FieldType.Nothing:
            errors.append(validation.type_missing(self.name, parent))

        return errors

    def to_dict(self):
        if self.value is None and not self.changes:
            return None

        result = dict(
            fn=self.name,
            ft=self.type.name,
            fv=self.value,
            fdn=self.displayName
        )

        if self.type == FieldType.Nothing:
            result["ft"] = "None"

        if self.modifications:
            result["md"] = [m.name for m in self.modifications]

        if self.changes:
            result["ch"] = self.changes

        return result

    def from_dict(self, field_dict):
        if not field_dict:
            return self

        self.name = field_dict.get('fn', None)
        self.value = field_dict.get('fv', None)
        self.displayName = field_dict.get('fdn', None)
        self.changes = field_dict.get('ch', dict())

        ft = field_dict.get('ft', 'None')
        self.type = FieldType.Nothing if ft == 'None' else FieldType[ft]

        md = field_dict.get('md', [])
        self.modifications = [Modification[m] for m in md]

        return self
