import unittest

from module import Root, Category, Field
from module.table import Cell, Head
from module.enum import FieldType, Modification


class TestRootValidateMethods(unittest.TestCase):
    def test_validate_with_table(self):
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

        errors = Root() \
            .add_general(Field(
                name="General1",
                type=FieldType.DateTime,
                value=5039466,
                displayName="Основное поле 1"
            )
        ) \
            .add_category(
                lambda c: c
                .set_properties(
                    name="Category1",
                    displayName="Категория 1",
                    modifications=[
                        Modification.Calendar, Modification.Help]
                )
                .add_table(
                    lambda t: t
                    .set_properties(
                        name="Table1",
                        displayName="Таблица 1"
                    )
                    .set_header(
                        lambda th: th
                        .add_cells([
                            Head(name="Head1", displayName="Header 1"),
                            Head(name="Head2", displayName="Header 2"),
                            Head(name="Head3", displayName="Header 3")
                        ]))
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
                    )
                )
                .add_field(Field(
                    type=FieldType.String,
                    name="Field1",
                    displayName="Поле категории 1",
                    value="просто строка",
                    modifications=[Modification.CurEUR]
                ))
        ) \
            .add_category(
                lambda c: c
                .set_properties(
                    name="Category2",
                    displayName="Категория 2",
                    modifications=[
                        Modification.Calendar, Modification.Help]
                )
                .add_field(Field(
                    type=FieldType.Price,
                    name="Field2",
                    displayName="Поле категории 2",
                    value=124124.54,
                    modifications=[Modification.CurEUR],
                    changes=dict([[1, "Старое поле категории 2"],
                                  [2, "Поле категории 2"]])
                ))
        ) \
            .validate()

        self.assertEqual(errors, [])

    def test_validate_with_array(self):
        errors = Root() \
            .add_category(
                lambda c: c
                .set_properties(
                    name="Category1",
                    displayName="Категория 1",
                    modifications=[Modification.Calendar, Modification.Help]
                )
                .add_field(Field(
                    type=FieldType.Price,
                    name="Field1",
                    displayName="Поле категории 1",
                    value=124124.54,
                    modifications=[Modification.CurEUR],
                    changes=dict([[1, "Старое поле категории 1"],
                                  [2, "Поле категории 1"]])
                ))
        ) \
            .add_category(
                lambda c: c
                .set_properties(
                    displayName="Category2",
                    name="Category2"
                )
                .add_array(
                    lambda ca: ca
                    .set_properties(
                        name="Array1",
                        displayName="Array 1"
                    )
                    .add_field(Field(
                        name="Field1",
                        displayName="Поле 1",
                        value="Значение 1",
                        type=FieldType.String
                    ))
                )
        ) \
            .validate()

        self.assertEqual(errors, [])

    def test_validate_with_customers(self):
        customers = [
            dict(
                regnum=1,
                guid="",
                name="test1",
                maxPrice=50325235.53,
                guaranteeApp=95830945.54
            ),
            dict(
                regnum=2,
                guid="",
                name="test2",
                maxPrice=20325235.53,
                guaranteeApp=35830945.54
            )
        ]

        errors = Root() \
            .add_customers(
                customers,
                lambda obj, c: c
                .set_properties(
                    max_price=obj['maxPrice'],
                    guarantee_app=obj['guaranteeApp'],
                    customer_name=obj['name'],
                    customer_guid=obj['guid']
                )
                .add_field(Field(
                    value="1",
                    name="test1",
                    displayName="fdn",
                    type=FieldType.String
                ))
        ) \
            .validate()

        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
