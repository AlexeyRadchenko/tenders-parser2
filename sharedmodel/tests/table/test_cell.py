import unittest
from json import dumps, loads

from module import Field
from module.table import Cell
from module.enum import FieldType


class TestCellClass(unittest.TestCase):
    def test_cell_and_field_are_same_except_fdn(self):
        field = Field(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        ).to_dict()

        field.pop('fdn', None)

        cell = Cell(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        ).to_dict()

        self.assertEqual(field, cell)

    def test_from_dict(self):
        cell1 = Cell(
            name="Price",
            type=FieldType.Integer,
            value=100,
            displayName="Цена"
        )
        cell1_dict = cell1.to_dict()

        clean_dict = loads(
            dumps(cell1_dict, ensure_ascii=False), encoding='utf-8')
        cell2 = Cell().from_dict(clean_dict)
        cell2_dict = cell2.to_dict()

        self.assertDictEqual(cell1_dict, cell2_dict)


if __name__ == '__main__':
    unittest.main()
