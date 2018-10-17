from .row import Row
from ..enum import Modification
from ..tools import validation, convert


class Body(object):
    def __init__(self, name=None, displayName=None, modifications=None):
        self.set_properties(name, displayName, modifications)
        self.type = "Table"
        self.head = Row()
        self.rows = []

    def set_properties(self, name=None, displayName=None, modifications=None):
        self.name = name
        self.displayName = displayName
        self.modifications = modifications if modifications else []
        return self

    def set_header(self, arg):
        if callable(arg):
            return self.set_header(arg(Row()))

        self.head = arg
        return self

    def add_row(self, arg):
        if callable(arg):
            return self.add_row(arg(Row()))

        self.rows.append(arg)
        return self

    def add_rows(self, arr, fun):
        if not arr:
            return self

        for item in arr:
            self.add_row(fun(item, Row()))
        return self

    def compare(self, *args):
        pass

    def validate(self, parent=None):
        errors = []

        errors.extend(self.head.validate('th', parent))
        errors.extend([
            e for i, c in enumerate(self.rows)
            for e in c.validate('tb.' + str(i), parent)
        ])

        if not self.name:
            errors.append(validation.name_missing(self.name, parent))
        if not self.head.cells:
            errors.append(validation.table_head_missing(self.name, parent))

        head_size = len(self.head.cells)
        if not all([len(r.cells) == head_size for r in self.rows]):
            errors.append(validation.table_denormalized(self.name, parent))

        return errors

    def to_dict(self):
        th = self.head.to_dict() if self.head else None
        tb = convert.list_to_dict(self.rows)

        if not tb:
            return None

        result = dict(
            fn=self.name,
            ft=self.type,
            fv=dict(
                th=th,
                tb=tb
            ),
            fdn=self.displayName
        )

        if self.modifications:
            result["md"] = [m.name for m in self.modifications]

        return result

    def from_dict(self, body_dict):
        if not body_dict:
            return self

        self.name = body_dict.get('fn', None)
        self.type = body_dict.get('ft', 'Table')
        self.displayName = body_dict.get('fdn', None)

        md = body_dict.get('md', [])
        self.modifications = [Modification[m] for m in md]

        fv = body_dict.get('fv', dict())
        th = fv.get('th', None)
        tb = fv.get('tb', dict())

        self.head = Row().from_dict('th', th)
        self.rows = [Row().from_dict('tb', v) for v in tb.values()]

        return self
