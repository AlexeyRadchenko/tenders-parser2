from enum import Enum, unique


@unique
class ValidationError(Enum):
    NameNotUnique = "children's names must be unique"
    NameNotSpecified = "name must be specified"
    DisplayNameNotSpecified = "displayName must be specified"
    TypeNotSpecified = "type must be specified"
    EmptyContainer = "must have at least one child"
    TableHeadMissing = "specify table head"
    TableDenormalized = "number of cells in head and rows must be equal"


def create(name, parent, error):
    child = name if name else '?'
    parent = parent + '.' if parent else ''
    message = error.value if isinstance(error, ValidationError) else error
    return parent + child + ': ' + message


def name_not_unique(name, parent=None):
    return create(name, parent, ValidationError.NameNotUnique)


def name_missing(name, parent=None):
    return create(name, parent, ValidationError.NameNotSpecified)


def display_name_missing(name, parent=None):
    return create(name, parent, ValidationError.DisplayNameNotSpecified)


def type_missing(name, parent=None):
    return create(name, parent, ValidationError.TypeNotSpecified)


def children_empty(name, parent=None):
    return create(name, parent, ValidationError.EmptyContainer)


def table_head_missing(name, parent=None):
    return create(name, parent, ValidationError.TableHeadMissing)


def table_denormalized(name, parent=None):
    return create(name, parent, ValidationError.TableDenormalized)


def inline(errors):
    return 'Validation errors:\r\n' + '\r\n'.join(errors)


def are_names_unique(entities):
    return len(entities) == len(set([c.name for c in entities]))


def validate_children(self_name, self_parent, entities):
    parent = next_parent(self_name, self_parent)
    return [e for c in entities for e in c.validate(parent)]


def next_parent(self_name, self_parent):
    self_name = self_name if self_name else '?'
    self_parent = self_parent + '.' if self_parent else ''
    return self_parent + self_name
