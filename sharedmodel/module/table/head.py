from ..tools import validation


class Head(object):
    def __init__(self, name=None, displayName=None):
        self.set_attributes(name, displayName)

    def set_attributes(self, name=None, displayName=None):
        self.name = name
        self.displayName = displayName
        return self

    def validate(self, parent):
        errors = []

        if not self.name:
            errors.append(validation.name_missing(self.name, parent))
        if not self.displayName:
            errors.append(validation.display_name_missing(self.name, parent))

        return errors

    def to_dict(self):
        return dict(
            fn=self.name,
            fv=self.displayName
        )

    def from_dict(self, head_dict):
        if not head_dict:
            return self
        self.name = head_dict.get('fn', None)
        self.displayName = head_dict.get('fv', None)
        return self
