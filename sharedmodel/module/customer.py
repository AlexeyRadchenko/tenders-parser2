from .field import Field
from .enum import FieldType
from .tools import validation, convert


class Customer(object):
    def __init__(self, max_price=None, guarantee_app=None, guarantee_contract=None, customer_guid=None, customer_name=None):
        self.entities = []
        self.name = customer_guid if customer_guid else customer_name
        self.set_properties(max_price, guarantee_app,
                            guarantee_contract, customer_guid, customer_name)

    def add_field(self, arg):
        if callable(arg):
            return self.add_field(arg(Field()))

        self.entities.append(arg)
        return self

    def add_max_price(self, max_price):
        self.entities.append(Field(
            value=max_price,
            name="maxPrice",
            displayName="Цена контракта",
            type=FieldType.Price
        ))
        return self

    def add_guarantee_app(self, guarantee_app):
        self.entities.append(Field(
            value=guarantee_app,
            name="guaranteeApp",
            displayName="Обеспечение заявки",
            type=FieldType.Price
        ))
        return self

    def add_guarantee_contract(self, guarantee_contract):
        self.entities.append(Field(
            value=guarantee_contract,
            name="guaranteeContract",
            displayName="Обеспечение контракта",
            type=FieldType.Price
        ))
        return self

    def add_customer_info(self, customer_guid, customer_name):
        self.name = customer_guid if customer_guid else customer_name
        self.entities.append(Field(
            value=dict(
                guid=customer_guid,
                name=customer_name
            ),
            name="customer",
            displayName="Заказчик",
            type=FieldType.Object
        ))
        return self

    def set_properties(self, max_price=None, guarantee_app=None, guarantee_contract=None, customer_guid=None, customer_name=None):
        if max_price:
            self.add_max_price(max_price)
        if guarantee_app:
            self.add_guarantee_app(guarantee_app)
        if guarantee_contract:
            self.add_guarantee_contract(guarantee_contract)
        if customer_guid or customer_name:
            self.add_customer_info(customer_guid, customer_name)
        return self

    def compare(self, other, other_date, self_date):
        for entity in self.entities:
            if entity.name == 'customer':
                continue
            other_entity = next(
                (e for e in other.entities if e.name == entity.name),
                None
            )
            if other_entity:
                entity.compare(other_entity, other_date, self_date)

    def validate(self, parent=None):
        errors = validation.validate_children(self.name, parent, self.entities)

        if not self.entities:
            errors.append(validation.children_empty(self.name, parent))
        if not validation.are_names_unique(self.entities):
            errors.append(validation.name_not_unique(self.name))

        return errors

    def to_dict(self):
        return convert.list_to_dict(self.entities)

    def from_dict(self, customer_dict):
        if not customer_dict:
            return self

        customer = next((v.get('fv', dict()) for v in customer_dict.values()
                         if v.get('fn', None) == 'customer'), dict())

        customer_guid = customer.get('guid', None)
        customer_name = customer.get('name', None)

        self.name = customer_guid if customer_guid else customer_name
        self.entities = [Field().from_dict(v) for v in customer_dict.values()]
        return self
