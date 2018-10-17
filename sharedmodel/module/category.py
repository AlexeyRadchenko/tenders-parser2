from .enum import Modification
from .field import Field
from .table import Body
from .tools import validation, convert


class Category(object):
    def __init__(self, name=None, displayName=None, type='Category', modifications=None, entities=None):
        self.name = name
        self.displayName = displayName
        self.modifications = modifications if modifications else []
        self.entities = entities if entities else []
        self.type = type

    def set_properties(self, name=None, displayName=None, modifications=None):
        self.name = name
        self.displayName = displayName
        self.modifications = modifications if modifications else []
        return self

    def add_field(self, arg):
        if callable(arg):
            return self.add_field(arg(Field()))

        self.entities.append(arg)
        return self

    def add_table(self, arg):
        if callable(arg):
            return self.add_table(arg(Body()))

        self.entities.append(arg)
        return self

    def add_array(self, arg):
        if callable(arg):
            return self.add_array(arg(Category(type='Array')))

        self.entities.append(arg)
        return self

    def compare(self, other, other_date, self_date):
        for entity in self.entities:
            other_entity = next(
                (e for e in other.entities if e.name == entity.name),
                None
            )
            if other_entity:
                entity.compare(other_entity, other_date, self_date)

    def validate(self, parent=None):
        errors = validation.validate_children(self.name, parent, self.entities)

        if not self.name:
            errors.append(validation.name_missing(self.name, parent))
        if not self.displayName:
            errors.append(validation.display_name_missing(self.name, parent))
        if not self.entities:
            errors.append(validation.children_empty(self.name, parent))
        if not validation.are_names_unique(self.entities):
            errors.append(validation.name_not_unique(self.name, parent))

        return errors

    def to_dict(self):
        fv = convert.list_to_dict(self.entities)

        if not fv:
            return None

        result = dict(
            fn=self.name,
            ft=self.type,
            fv=fv,
            fdn=self.displayName,
        )

        if self.modifications:
            result["md"] = [m.name for m in self.modifications]

        return result

    def from_dict(self, category_dict):
        if not category_dict:
            return self
        self.name = category_dict.get('fn', None)
        self.type = category_dict.get('ft', 'Category')
        self.displayName = category_dict.get('fdn', None)

        md = category_dict.get('md', [])
        self.modifications = [Modification[m] for m in md]

        self.entities = []
        entites_dict = category_dict.get('fv', dict())
        for entity_dict in entites_dict.values():
            ft = entity_dict.get('ft', 'None')
            entity = None
            if ft == 'Category':
                entity = Category().from_dict(entity_dict)
            elif ft == 'Table':
                entity = Body().from_dict(entity_dict)
            elif ft == 'Array':
                entity = Category(type='Array').from_dict(entity_dict)
            else:
                entity = Field().from_dict(entity_dict)
            if entity:
                self.entities.append(entity)
        return self

