import unittest
from json import dumps, loads

from module import Category, Field
from module.enum import FieldType


class TestCategoryClass(unittest.TestCase):
    def test_add_field_object(self):
        obj = Category(name="Category1") \
            .add_field(Field(
                name="Price",
                displayName="Цена",
                value=1
            )) \
            .add_field(Field(
                name="Date",
                displayName="Дата",
                value=1
            ))
        self.assertEqual(len(obj.entities), 2)

    def test_add_field_lambda(self):
        obj = Category(name="Category1") \
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

    def test_to_dict_returns_none_when_entities_are_empty(self):
        obj = Category(name="Category1")
        self.assertEqual(obj.to_dict(), None)

    def test_to_dict_returns_valid_dictionary(self):
        obj = Category(name="Category1") \
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
        expected = "{\"fn\": \"Category1\", \"ft\": \"Category\", \"fv\": {\"0\": {\"fn\": \"Price\", \"ft\": \"None\", \"fv\": 1, \"fdn\": \"Цена\"}, \"1\": {\"fn\": \"Date\", \"ft\": \"None\", \"fv\": 1, \"fdn\": \"Дата\"}}, \"fdn\": null}"
        self.assertEqual(dumps(obj.to_dict(), ensure_ascii=False), expected)

    def test_compare_same_fields(self):
        obj1 = Category(name="Category1") \
            .add_field(lambda f: f.set_properties(
                name="Price",
                displayName="Цена",
                value=100
            )) \
            .add_field(lambda f: f.set_properties(
                name="Date",
                displayName="Дата",
                value=100
            ))

        obj2 = Category(name="Category1") \
            .add_field(lambda f: f.set_properties(
                name="Price",
                displayName="Цена",
                value=200
            )) \
            .add_field(lambda f: f.set_properties(
                name="Date",
                displayName="Дата",
                value=200
            ))

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
        obj1 = Category(name="Category1") \
            .add_field(lambda f: f.set_properties(
                name="Price",
                displayName="Цена",
                value=100
            )) \
            .add_field(lambda f: f.set_properties(
                name="Date",
                displayName="Дата",
                value=100
            ))

        obj2 = Category(name="Category1") \
            .add_field(lambda f: f.set_properties(
                name="Price",
                displayName="Цена",
                value=200
            ))

        obj1.compare(obj2, 200, 100)
        self.assertEqual(obj1.entities[0].changes, dict([
            [200, 200],
            [100, 100]
        ]))
        self.assertEqual(obj1.entities[1].changes, dict())

    def test_validate_invalid(self):
        errors = Category().validate("parent")
        self.assertEqual(len(errors), 3)

    def test_validate_invalid_with_name(self):
        errors = Category(name="category").validate("parent")
        self.assertEqual(len(errors), 2)
        self.assertTrue(
            all([e.startswith("parent.category:") for e in errors]))

    def test_validate_invalid_child(self):
        errors = Category(name="category", displayName="Category 1") \
            .add_field(Field(
                name="field",
                type=FieldType.Date,
                value=1,
            )) \
            .validate("parent")

        self.assertEqual(len(errors), 1)
        self.assertTrue(
            all([e.startswith("parent.category.field:") for e in errors]))

    def test_validate_invalid_non_unique_children(self):
        errors = Category(name="category", displayName="Category 1") \
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
        errors = Category(name="category", displayName="Category 1") \
            .add_field(Field(
                name="field",
                displayName="Field 1",
                type=FieldType.Date,
                value=1,
            )) \
            .validate("parent")

        self.assertEqual(len(errors), 0)

    def test_from_dict_fields(self):
        obj1 = Category(
            name="Category1",
            displayName="Category 1",
        ) \
            .add_field(lambda f: f.set_properties(
                name="Price",
                type=FieldType.Price,
                displayName="Цена",
                value=1
            )) \
            .add_field(lambda f: f.set_properties(
                name="Date",
                type=FieldType.Date,
                displayName="Дата",
                value=1
            ))
        obj1_dict = obj1.to_dict()

        clean_dict = loads(
            dumps(obj1_dict, ensure_ascii=False), encoding='utf-8')
        obj2 = Category().from_dict(clean_dict)
        obj2_dict = obj2.to_dict()

        self.assertDictEqual(obj1_dict, obj2_dict)

    def test_from_dict_array(self):
        obj1_dict = Category(
            name="Category1",
            displayName="Category 1",
        ) \
            .add_field(lambda f: f.set_properties(
                name="Price",
                type=FieldType.Price,
                displayName="Цена",
                value=1
            )) \
            .add_array(
                lambda a: a.set_properties(
                    name="Array1",
                    displayName="Array 1",
                )
                .add_field(lambda f: f.set_properties(
                    name="Price",
                    type=FieldType.Price,
                    displayName="Цена",
                    value=1
                ))
                .add_field(lambda f: f.set_properties(
                    name="Date",
                    type=FieldType.Date,
                    displayName="Дата",
                    value=1
                ))
        ).to_dict()

        clean_dict = loads(
            dumps(obj1_dict, ensure_ascii=False), encoding='utf-8')
        obj2_dict = Category().from_dict(clean_dict).to_dict()

        self.assertDictEqual(obj1_dict, obj2_dict)


if __name__ == '__main__':
    unittest.main()
