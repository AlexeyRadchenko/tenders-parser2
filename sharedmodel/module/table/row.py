from ..tools import validation, convert
from .cell import Cell
from .head import Head


class Row(object):
    def __init__(self):
        self.cells = []

    def add_cell(self, cell):
        self.cells.append(cell)
        return self

    def add_cells(self, cells):
        self.cells.extend(cells)
        return self

    def validate(self, name=None, parent=None):
        errors = validation.validate_children(name, parent, self.cells)
        if not validation.are_names_unique(self.cells):
            errors.append(validation.name_not_unique(name, parent))
        return errors

    def to_dict(self):
        return convert.list_to_dict(self.cells)

    def from_dict(self, type, row_dict):
        if not row_dict:
            return self
        self.cells = []
        for cell_dict in row_dict.values():
            if type == 'th':
                self.cells.append(Head().from_dict(cell_dict))
            else:
                self.cells.append(Cell().from_dict(cell_dict))
        return self
