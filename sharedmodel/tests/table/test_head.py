import unittest
from json import dumps, loads

from module.table import Head
from module.enum import FieldType


class TestHeadClass(unittest.TestCase):
    def test_to_dict_returns_only_two_fields(self):
        obj = Head(
            name="Price",
            displayName="Цена"
        )

        expected = dict(
            fn="Price",
            fv="Цена"
        )
        self.assertEqual(obj.to_dict(), expected)

    def test_to_dict_convertable_to_dict(self):
        obj = Head(
            name="Price",
            displayName="Цена"
        )

        expected = dumps(dict(
            fn="Price",
            fv="Цена"
        ))
        self.assertEqual(dumps(obj.to_dict()), expected)

    def test_from_dict(self):
        obj1_dict = Head(
            name="Price",
            displayName="Цена"
        ).to_dict()

        clean_dict = loads(
            dumps(obj1_dict, ensure_ascii=False), encoding='utf-8')
        obj2_dict = Head().from_dict(clean_dict).to_dict()

        self.assertDictEqual(obj1_dict, obj2_dict)


if __name__ == '__main__':
    unittest.main()
