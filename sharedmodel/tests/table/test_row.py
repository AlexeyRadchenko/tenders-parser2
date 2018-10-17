import unittest
from json import dumps, loads

from module.table import Row, Cell, Head
from module.enum import FieldType


class TestRowClass(unittest.TestCase):
    def test_add_cell_appends_cell(self):
        obj = Row() \
            .add_cell(Cell(
                name="Price",
                value=1
            )) \
            .add_cell(Cell(
                name="Date",
                value=1
            ))
        self.assertEqual(len(obj.cells), 2)

    def test_add_cell_appends_head(self):
        obj = Row() \
            .add_cell(Head(
                name="Price",
                displayName="Цена",
            )) \
            .add_cell(Head(
                name="Date",
                displayName="Дата",
            ))
        self.assertEqual(len(obj.cells), 2)

    def test_add_cells_extends_cells(self):
        obj = Row() \
            .add_cell(Cell(
                name="Price",
                displayName="Цена",
                value=1
            )) \
            .add_cells([
                Cell(
                    name="Date",
                    value=1
                ),
                Cell(
                    name="Date2",
                    value=2
                )
            ])
        self.assertEqual(len(obj.cells), 3)

    def test_to_dict_returns_only_cells(self):
        obj = Row() \
            .add_cells([
                Cell(
                    name="Price",
                    value=1
                ),
                Cell(
                    name="Date",
                    value=1
                )
            ]) \
            .to_dict()

        self.assertEqual(len(obj.keys()), 2)

    def test_to_dict_cell_convertable_to_dict(self):
        obj = Row() \
            .add_cells([
                Cell(
                    name="Price",
                    value=1
                ),
                Cell(
                    name="Date",
                    value=1
                )
            ]) \
            .to_dict()

        expected = "{\"0\": {\"fn\": \"Price\", \"ft\": \"None\", \"fv\": 1}, \"1\": {\"fn\": \"Date\", \"ft\": \"None\", \"fv\": 1}}"
        self.assertEqual(dumps(obj, ensure_ascii=False), expected)

    def test_to_dict_head_convertable_to_dict(self):
        obj = Row() \
            .add_cells([
                Head(
                    name="Price",
                    displayName="Цена",
                ),
                Head(
                    name="Date",
                    displayName="Дата",
                )
            ]) \
            .to_dict()

        expected = "{\"0\": {\"fn\": \"Price\", \"fv\": \"Цена\"}, \"1\": {\"fn\": \"Date\", \"fv\": \"Дата\"}}"
        self.assertEqual(dumps(obj, ensure_ascii=False), expected)

    def test_validate_invalid_children(self):
        errors = Row() \
            .add_cells([
                Head(
                    name="Price",
                ),
                Head(
                    name="Date",
                )
            ]) \
            .validate("tb.0", "parent")
        self.assertEqual(len(errors), 2)
        self.assertTrue(all([e.startswith('parent.tb.0.') for e in errors]))

    def test_validate_invalid_children_not_unique(self):
        errors = Row() \
            .add_cells([
                Head(
                    name="Price",
                    displayName="Цена",
                ),
                Head(
                    name="Price",
                    displayName="Цена",
                )
            ]) \
            .validate("tb.0", "parent")
        self.assertEqual(len(errors), 1)
        self.assertTrue(all([e.startswith('parent.tb.0:') for e in errors]))

    def test_from_dict_empty(self):
        row1_dict = Row().to_dict()

        clean_dict = loads(
            dumps(row1_dict, ensure_ascii=False), encoding='utf-8')
        row2_dict = Row().from_dict('tb', clean_dict).to_dict()

        self.assertDictEqual(row1_dict, row2_dict)

    def test_from_dict_tb(self):
        row1_dict = Row() \
            .add_cells([
                Cell(
                    name="Price",
                    value=1,
                    type=FieldType.Price
                ),
                Cell(
                    name="Date",
                    value=1,
                    type=FieldType.Date
                )
            ]) \
            .to_dict()

        clean_dict = loads(
            dumps(row1_dict, ensure_ascii=False), encoding='utf-8')
        row2_dict = Row().from_dict('tb', clean_dict).to_dict()

        self.assertDictEqual(row1_dict, row2_dict)

    def test_from_dict_th(self):
        row1_dict = Row() \
            .add_cells([
                Head(
                    name="Price",
                    displayName="Цена",
                ),
                Head(
                    name="Price",
                    displayName="Цена",
                )
            ]) \
            .to_dict()

        clean_dict = loads(
            dumps(row1_dict, ensure_ascii=False), encoding='utf-8')
        row2_dict = Row().from_dict('th', clean_dict).to_dict()

        self.assertDictEqual(row1_dict, row2_dict)


if __name__ == '__main__':
    unittest.main()
