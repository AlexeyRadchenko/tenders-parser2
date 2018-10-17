import unittest
from json import dumps, loads

from module import Field
from module.enum import FieldType


class TestFieldClass(unittest.TestCase):
    def test_to_dict_returns_none_when_value_not_set(self):
        obj = Field()
        self.assertEqual(obj.to_dict(), None)

    def test_to_dict_return_valid_dict(self):
        obj = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )

        expected = dict(
            fn="Price",
            ft=FieldType.Integer.name,
            fv=100,
            fdn="Цена"
        )
        self.assertEqual(obj.to_dict(), expected)

    def test_to_dict_convertable_to_dict(self):
        obj = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )

        expected = dumps(dict(
            fn="Price",
            ft=FieldType.Integer.name,
            fv=100,
            fdn="Цена"
        ))

        self.assertEqual(dumps(obj.to_dict()), expected)

    def test_compare_same(self):
        obj1 = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )
        obj2 = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )
        obj1.compare(obj2, 100, 200)

        self.assertEqual(obj1.changes, dict())

    def test_compare_different(self):
        obj1 = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )
        obj2 = Field(
            name="Price",
            type=FieldType.Integer,
            value=50,
            displayName="Цена"
        )
        obj1.compare(obj2, 100, 200)

        self.assertEqual(obj1.changes, dict([
            [100, 50],
            [200, 100]
        ]))

    def test_validate_no_parent(self):
        errors = Field().validate()

        self.assertEqual(len(errors), 3)
        self.assertTrue(all([e.startswith("?: ") for e in errors]))

    def test_validate_parent(self):
        errors = Field().validate("parent")

        self.assertEqual(len(errors), 3)
        self.assertTrue(all([e.startswith("parent.?: ") for e in errors]))

    def test_validate_name_specified_no_parent(self):
        errors = Field(name="child").validate()

        self.assertEqual(len(errors), 2)
        self.assertTrue(all([e.startswith("child: ") for e in errors]))

    def test_validate_name_specified_with_parent(self):
        errors = Field(name="child").validate("parent")

        self.assertEqual(len(errors), 2)
        self.assertTrue(all([e.startswith("parent.child: ") for e in errors]))

    def test_validate_valid_empty(self):
        errors = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        ).validate()

        self.assertEqual(len(errors), 0)

    def test_from_dict_return_valid_dict(self):
        obj1 = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )
        obj1_dict = obj1.to_dict()

        clean_dict = loads(
            dumps(obj1_dict, ensure_ascii=False), encoding="utf-8")
        obj2 = Field().from_dict(clean_dict)
        obj2_dict = obj2.to_dict()

        self.assertDictEqual(obj1_dict, obj2_dict)


if __name__ == '__main__':
    unittest.main()
