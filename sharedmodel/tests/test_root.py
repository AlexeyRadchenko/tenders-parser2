import unittest

from sharedmodel.module import Root, Category, Field
from sharedmodel.module.table import Cell, Head
from sharedmodel.module.enum import FieldType, Modification


class TestRootMethods(unittest.TestCase):

    def test_add_general_object(self):
        field = Field(
            name="General1",
            type=FieldType.DateTime,
            value=5039466,
            displayName="Основное поле 1"
        )
        obj = Root().add_general(field)
        self.assertEqual(obj.general[0], field)

    def test_add_general_lambda(self):
        obj = Root().add_general(lambda f: f.set_properties(
            name="General1",
            type=FieldType.DateTime,
            value=5039466,
            displayName="Основное поле 1"
        ))
        self.assertIsNotNone(obj.general[0])
        self.assertIsInstance(obj.general[0], Field)

    def test_add_category_object(self):
        category = Category(
            name="Category1",
            displayName="Категория 1",
            modifications=[
                Modification.Calendar, Modification.Help]
        )
        obj = Root().add_category(category)
        self.assertEqual(obj.containers[0], category)

    def test_add_category_lambda(self):
        obj = Root().add_category(
            lambda c: c
            .set_properties(
                name="Category1",
                displayName="Категория 1",
                modifications=[
                    Modification.Calendar, Modification.Help]
            )
        )
        self.assertIsNotNone(obj.containers[0])
        self.assertIsInstance(obj.containers[0], Category)


if __name__ == '__main__':
    unittest.main()
