# TODO check publication_date
from json import dumps, loads
from copy import deepcopy
from .category import Category
from .customer import Customer
from .field import Field
from .tools import validation, convert


class Root(object):
    def __init__(self, publication_date=None):
        self.general = []
        self.containers = []
        self.customers = []
        self.publication_date = publication_date

    # todo constructor from json

    #   @staticmethod
    #   def generate_customers(self, customers):
    #     pass

    #   @staticmethod
    #   def generate_containers(self, containers):
    #     pass

    #   @staticmethod
    #   def get_contacts(self, contacts):
    #     pass

    #   @staticmethod
    #   def generate_general(self, general):
    #     pass

    #   @staticmethod
    #   def form_changes(self, changes):
    #     pass

    #   @staticmethod
    #   def form_modifications(self, type):
    #     pass

    #   @staticmethod
    #   def get_field_type(self, type):
    #     pass

    def add_customer(self, arg):
        if callable(arg):
            return self.add_customer(arg(Customer()))

        self.customers.append(arg)
        return self

    def add_customers(self, customers, fun):
        if not customers:
            return self

        for customer in customers:
            self.add_customer(fun(customer, Customer()))

        return self

    def set_publication_date(self, date):
        self.publication_date = date
        return self

    def add_general(self, arg):
        # method overload to pass lambda function that accepts field
        if callable(arg):
            return self.add_general(arg(Field()))

        # pass field directly
        self.general.append(arg)
        return self

    def add_category(self, arg):
        # method overload to pass lambda function that accepts category
        if callable(arg):
            return self.add_category(arg(Category()))

        # pass category directly
        self.containers.append(arg)
        return self

    def compare(self, other):
        for general in self.general:
            other_general = next(
                (o for o in other.general if o.name == general.name), None)
            if other_general:
                general.compare(
                    other_general, other.publication_date, self.publication_date)

        for customer in self.customers:
            other_customer = next(
                (o for o in other.customers if o.name == customer.name), None)
            if other_customer:
                customer.compare(
                    other_customer, other.publication_date, self.publication_date)

        for container in self.containers:
            other_container = next(
                (o for o in other.containers if o.name == container.name), None)
            if other_container:
                container.compare(
                    other_container, other.publication_date, self.publication_date)

        return self

    def compare_many(self, others):
        if not self.publication_date or not all([o.publication_date for o in others]):
            raise Exception(
                message="Publication date must be specified")

        result = deepcopy(self)
        for other in sorted(others, key=lambda o: o.publication_date):
            result.compare(other)
        return result

    def validate(self):
        errors = []

        errors.extend(validation.validate_children(
            "root.general", None, self.general))
        errors.extend(validation.validate_children(
            "root.general.customers", None, self.customers))
        errors.extend(validation.validate_children(
            "root", None, self.containers))

        if not validation.are_names_unique(self.general):
            errors.append(validation.name_not_unique("root.general"))

        if not validation.are_names_unique(self.containers):
            errors.append(validation.name_not_unique("root"))

        if not validation.are_names_unique(self.customers):
            errors.append(validation.name_not_unique("root.general.customers"))

        return errors

    def to_dict(self):
        errors = self.validate()
        if errors:
            raise Exception(message=validation.inline(errors))

        result = convert.list_to_dict(self.containers)
        result['general'] = convert.list_to_dict(self.general)
        result['general']['customers'] = convert.list_to_array(self.customers)

        return result

    def to_json(self):
        return dumps(self.to_dict(), ensure_ascii=False, separators=(',', ':'))

    def from_dict(self, root_dict):
        general = root_dict.pop('general', dict())
        customers = general.pop('customers', [])

        self.general = [Field().from_dict(v) for v in general.values()]
        self.customers = [Customer().from_dict(v) for v in customers]
        self.containers = [Category().from_dict(v) for v in root_dict.values()]
        return self

    def from_json(self, root_json):
        root_dict = loads(root_json, encoding="utf-8")
        return self.from_dict(root_dict)
