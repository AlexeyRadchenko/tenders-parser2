import unittest
from json import dumps, loads

from module import Customer, Field
from module.enum import FieldType


class TestCustomerClass(unittest.TestCase):
    def test_set_properties_working_as_expected(self):
        obj1 = Customer() \
            .add_max_price(100) \
            .add_guarantee_app(5) \
            .add_guarantee_contract(10) \
            .add_customer_info("1", "Customer 1") \
            .to_dict()

        obj2 = Customer() \
            .set_properties(
                max_price=100,
                guarantee_app=5,
                guarantee_contract=10,
                customer_guid="1",
                customer_name="Customer 1"
        ) \
            .to_dict()

        self.assertEqual(obj1, obj2)

    def test_add_field_lambda(self):
        obj = Customer() \
            .add_field(lambda f: f.set_properties(
                name="Price",
                displayName="Цена",
                value=1
            )) \
            .add_field(lambda f: f.set_properties(
                name="Date",
                displayName="Дата",
                value=1
            ))
        self.assertEqual(len(obj.entities), 2)

    def test_to_dict_returns_empty_dict_when_entities_are_empty(self):
        obj = Customer()
        self.assertEqual(obj.to_dict(), dict())

    def test_to_dict_returns_valid_dictionary(self):
        obj = Customer() \
            .set_properties(
                max_price=100,
                guarantee_app=5,
                guarantee_contract=10,
                customer_guid="1",
                customer_name="Customer 1"
        ) \
            .to_dict()

        expected = "{\"0\": {\"fn\": \"maxPrice\", \"ft\": \"Price\", \"fv\": 100, \"fdn\": \"Цена контракта\"}, \"1\": {\"fn\": \"guaranteeApp\", \"ft\": \"Price\", \"fv\": 5, \"fdn\": \"Обеспечение заявки\"}, \"2\": {\"fn\": \"guaranteeContract\", \"ft\": \"Price\", \"fv\": 10, \"fdn\": \"Обеспечение контракта\"}, \"3\": {\"fn\": \"customer\", \"ft\": \"Object\", \"fv\": {\"guid\": \"1\", \"name\": \"Customer 1\"}, \"fdn\": \"Заказчик\"}}"
        self.assertEqual(dumps(obj, ensure_ascii=False), expected)

    def test_compare_same_fields(self):
        obj1 = Customer() \
            .add_max_price(100) \
            .add_guarantee_app(100)

        obj2 = Customer() \
            .add_max_price(200) \
            .add_guarantee_app(200)

        obj1.compare(obj2, 200, 100)
        self.assertEqual(obj1.entities[0].changes, dict([
            [200, 200],
            [100, 100]
        ]))
        self.assertEqual(obj1.entities[1].changes, dict([
            [200, 200],
            [100, 100]
        ]))

    def test_compare_missing_field(self):
        obj1 = Customer() \
            .add_max_price(100) \
            .add_guarantee_app(100)

        obj2 = Customer() \
            .add_max_price(200)

        obj1.compare(obj2, 200, 100)
        self.assertEqual(obj1.entities[0].changes, dict([
            [200, 200],
            [100, 100]
        ]))
        self.assertEqual(obj1.entities[1].changes, dict())

    def test_validate_invalid(self):
        errors = Customer().validate("parent")
        self.assertEqual(len(errors), 1)

    def test_validate_invalid_child(self):
        errors = Customer(customer_name='customer') \
            .add_field(Field(
                name="field",
                type=FieldType.Date,
                value=1,
            )) \
            .validate("parent")

        self.assertEqual(len(errors), 1)
        self.assertTrue(
            all([e.startswith("parent.customer.field:") for e in errors]))

    def test_validate_invalid_non_unique_children(self):
        errors = Customer() \
            .add_field(Field(
                name="field",
                displayName="Field 1",
                type=FieldType.Date,
                value=1,
            )) \
            .add_field(Field(
                name="field",
                displayName="Field 2",
                type=FieldType.Date,
                value=2,
            )) \
            .validate("parent")
        self.assertEqual(len(errors), 1)

    def test_validate_valid(self):
        errors = Customer() \
            .add_field(Field(
                name="field",
                displayName="Field 1",
                type=FieldType.Date,
                value=1,
            )) \
            .validate("parent")

        self.assertEqual(len(errors), 0)

    def test_from_dict_valid(self):
        obj1_dict = Customer() \
            .set_properties(customer_name="customer") \
            .add_field(Field(
                name="field",
                displayName="Field 1",
                type=FieldType.Date,
                value=1,
            )) \
            .to_dict()

        clean_dict = loads(
            dumps(obj1_dict, ensure_ascii=False), encoding='utf-8')
        obj2 = Customer().from_dict(clean_dict)
        obj2_dict = obj2.to_dict()

        self.assertEqual(obj2.name, 'customer')
        self.assertDictEqual(obj1_dict, obj2_dict)


if __name__ == '__main__':
    unittest.main()
