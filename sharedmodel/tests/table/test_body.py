import unittest
from json import dumps, loads
from copy import deepcopy

from module.enum import FieldType
from module.table import Body, Row, Cell, Head
from module.enum import FieldType


class TestBodyClass(unittest.TestCase):
    def test_set_header_object(self):
        row = Row()
        obj = Body(name="Table1").set_header(row)

        self.assertEqual(obj.head, row)

    def test_set_header_lmabda(self):
        obj = Body(name="Table1").set_header(lambda r: r)

        self.assertIsNotNone(obj.head)
        self.assertIsInstance(obj.head, Row)

    def test_add_row_object(self):
        obj = Body(name="Table1").add_row(Row()).add_row(Row())

        self.assertEqual(len(obj.rows), 2)
        self.assertIsInstance(obj.rows[0], Row)

    def test_add_row_lambda(self):
        obj = Body(name="Table1").add_row(lambda r: r).add_row(lambda r: r)

        self.assertEqual(len(obj.rows), 2)
        self.assertIsInstance(obj.rows[0], Row)

    def test_add_rows_none(self):
        obj = Body(name="Table1").add_rows(None, lambda v, r: r)

        self.assertEqual(len(obj.rows), 0)

    def test_add_rows_list(self):
        lst = [dict(a=1, b=2), dict(a=2, b=1)]
        obj = Body(name="Table1").add_rows(
            lst,
            lambda v, r: r
        )

        self.assertEqual(len(obj.rows), 2)
        self.assertIsInstance(obj.rows[0], Row)

    def test_to_dict_returns_none_when_rows_are_empty(self):
        obj = Body(name="Table1").set_header(Row())
        self.assertEqual(obj.to_dict(), None)

    def test_to_dict_returns_valid_dictionary(self):
        some_list = [
            dict(
                SomeShit="SomeShit1",
                Nullable="Nullable1",
                OtherShit=1
            ),
            dict(
                SomeShit="SomeShit2",
                Nullable="Nullable2",
                OtherShit=2
            ),
            dict(
                SomeShit="SomeShit2",
                Nullable="Nullable2",
                OtherShit=2
            )
        ]

        obj = Body() \
            .set_properties(
                name="Table1",
                displayName="Таблица 1"
        ) \
            .set_header(
                lambda th: th
                .add_cells([
                    Head(name="Head1", displayName="Header 1"),
                    Head(name="Head2", displayName="Header 2"),
                    Head(name="Head3", displayName="Header 3")
                ])
        ) \
            .add_rows(
                some_list,
                lambda list_element, row: row.add_cells([
                    Cell(
                        name="Head1",
                        type=FieldType.Integer,
                        value=list_element['SomeShit']
                    ),
                    Cell(
                        name="Head2",
                        type=FieldType.String,
                        value=list_element['Nullable']
                    ),
                    Cell(
                        name="Head3",
                        type=FieldType.String,
                        value=list_element['OtherShit']
                    )
                ])
        ) \
            .to_dict()

        # print(dumps(obj, ensure_ascii=False))
        expected = "{\"fn\": \"Table1\", \"ft\": \"Table\", \"fv\": {\"th\": {\"0\": {\"fn\": \"Head1\", \"fv\": \"Header 1\"}, \"1\": {\"fn\": \"Head2\", \"fv\": \"Header 2\"}, \"2\": {\"fn\": \"Head3\", \"fv\": \"Header 3\"}}, \"tb\": {\"0\": {\"0\": {\"fn\": \"Head1\", \"ft\": \"Integer\", \"fv\": \"SomeShit1\"}, \"1\": {\"fn\": \"Head2\", \"ft\": \"String\", \"fv\": \"Nullable1\"}, \"2\": {\"fn\": \"Head3\", \"ft\": \"String\", \"fv\": 1}}, \"1\": {\"0\": {\"fn\": \"Head1\", \"ft\": \"Integer\", \"fv\": \"SomeShit2\"}, \"1\": {\"fn\": \"Head2\", \"ft\": \"String\", \"fv\": \"Nullable2\"}, \"2\": {\"fn\": \"Head3\", \"ft\": \"String\", \"fv\": 2}}, \"2\": {\"0\": {\"fn\": \"Head1\", \"ft\": \"Integer\", \"fv\": \"SomeShit2\"}, \"1\": {\"fn\": \"Head2\", \"ft\": \"String\", \"fv\": \"Nullable2\"}, \"2\": {\"fn\": \"Head3\", \"ft\": \"String\", \"fv\": 2}}}}, \"fdn\": \"Таблица 1\"}"
        self.assertEqual(dumps(obj, ensure_ascii=False), expected)

    def test_compare_pass(self):
        obj1 = Body(name="Table1").set_header(lambda r: r)
        obj2 = Body(name="Table1").set_header(lambda r: r)

        obj1_before = deepcopy(obj1)

        obj1.compare(obj2, 200, 100)
        self.assertEqual(obj1.to_dict(), obj1_before.to_dict())

    def test_validate_empty(self):
        errors = Body().validate("parent")
        self.assertEqual(len(errors), 2)

    def test_validate_head_missing(self):
        errors = Body(name="table").validate("parent")
        self.assertEqual(len(errors), 1)

    def test_validate_row_size_mismatch(self):
        errors = Body(name="table") \
            .set_header(lambda f: f.add_cells([
                Head(name="h1", displayName="Head 1"),
                Head(name="h2", displayName="Head 2")
            ])) \
            .add_row(lambda r: r.add_cell(Cell(
                name="h1",
                type=FieldType.Date,
                value=1
            ))) \
            .validate("parent")

        self.assertEqual(len(errors), 1)

    def test_validate_deep_error(self):
        errors = Body(name="table") \
            .set_header(lambda f: f.add_cells([
                Head(name="h1", displayName="Head 1"),
                Head(name="h2", displayName="Head 2")
            ])) \
            .add_row(lambda r: r.add_cells([
                Cell(
                    name="h1",
                    type=FieldType.Date,
                    value=1
                ),
                Cell(
                    name="h2",
                    value=2
                )
            ])) \
            .validate("parent")

        self.assertEqual(len(errors), 1)

    def test_from_dict(self):
        bd1_dict = Body(name="table", displayName="Table 1") \
            .set_header(lambda f: f.add_cells([
                Head(name="h1", displayName="Head 1"),
                Head(name="h2", displayName="Head 2")
            ])) \
            .add_row(lambda r: r.add_cells([
                Cell(
                    name="h1",
                    type=FieldType.Date,
                    value=1
                ),
                Cell(
                    name="h2",
                    type=FieldType.Date,
                    value=2
                )
            ])) \
            .add_row(lambda r: r.add_cells([
                Cell(
                    name="h1",
                    type=FieldType.Date,
                    value=3
                ),
                Cell(
                    name="h2",
                    type=FieldType.Date,
                    value=4
                )
            ])) \
            .to_dict()

        clean_dict = loads(
            dumps(bd1_dict, ensure_ascii=False), encoding='utf-8')
        bd2_dict = Body().from_dict(clean_dict).to_dict()

        self.assertDictEqual(bd1_dict, bd2_dict)


if __name__ == '__main__':
    unittest.main()
