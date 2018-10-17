import unittest

from module import Root, Category, Field
from module.table import Cell, Head
from module.enum import FieldType, Modification


class TestRootToJsonMethods(unittest.TestCase):
    def test_to_json_with_table(self):
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

        result = Root() \
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
            .to_json()

        self.assertEqual(
            result, "{\"0\":{\"fn\":\"Category1\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Table1\",\"ft\":\"Table\",\"fv\":{\"th\":{\"0\":{\"fn\":\"Head1\",\"fv\":\"Header 1\"},\"1\":{\"fn\":\"Head2\",\"fv\":\"Header 2\"},\"2\":{\"fn\":\"Head3\",\"fv\":\"Header 3\"}},\"tb\":{\"0\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit1\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable1\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":1}},\"1\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit2\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable2\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":2}},\"2\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit2\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable2\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":2}}}},\"fdn\":\"Таблица 1\"},\"1\":{\"fn\":\"Field1\",\"ft\":\"String\",\"fv\":\"просто строка\",\"fdn\":\"Поле категории 1\",\"md\":[\"CurEUR\"]}},\"fdn\":\"Категория 1\",\"md\":[\"Calendar\",\"Help\"]},\"1\":{\"fn\":\"Category2\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Field2\",\"ft\":\"Price\",\"fv\":124124.54,\"fdn\":\"Поле категории 2\",\"md\":[\"CurEUR\"],\"ch\":{\"1\":\"Старое поле категории 2\",\"2\":\"Поле категории 2\"}}},\"fdn\":\"Категория 2\",\"md\":[\"Calendar\",\"Help\"]},\"general\":{\"0\":{\"fn\":\"General1\",\"ft\":\"DateTime\",\"fv\":5039466,\"fdn\":\"Основное поле 1\"},\"customers\":[]}}")

    def test_to_json_with_array(self):
        result = Root() \
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
            .to_json()

        expected = "{\"0\":{\"fn\":\"Category1\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Field1\",\"ft\":\"Price\",\"fv\":124124.54,\"fdn\":\"Поле категории 1\",\"md\":[\"CurEUR\"],\"ch\":{\"1\":\"Старое поле категории 1\",\"2\":\"Поле категории 1\"}}},\"fdn\":\"Категория 1\",\"md\":[\"Calendar\",\"Help\"]},\"1\":{\"fn\":\"Category2\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Array1\",\"ft\":\"Array\",\"fv\":{\"0\":{\"fn\":\"Field1\",\"ft\":\"String\",\"fv\":\"Значение 1\",\"fdn\":\"Поле 1\"}},\"fdn\":\"Array 1\"}},\"fdn\":\"Category2\"},\"general\":{\"customers\":[]}}"
        self.assertEqual(result, expected)

    def test_to_json_with_customers(self):
        customers = [
            dict(
                regnum=str(1),
                guid="",
                name="test1",
                maxPrice=50325235.53,
                guaranteeApp=95830945.54
            ),
            dict(
                regnum=str(2),
                guid="",
                name="test2",
                maxPrice=20325235.53,
                guaranteeApp=35830945.54
            )
        ]

        result = Root() \
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
            .to_json()

        expected = "{\"general\":{\"customers\":[{\"0\":{\"fn\":\"maxPrice\",\"ft\":\"Price\",\"fv\":50325235.53,\"fdn\":\"Цена контракта\"},\"1\":{\"fn\":\"guaranteeApp\",\"ft\":\"Price\",\"fv\":95830945.54,\"fdn\":\"Обеспечение заявки\"},\"2\":{\"fn\":\"customer\",\"ft\":\"Object\",\"fv\":{\"guid\":\"\",\"name\":\"test1\"},\"fdn\":\"Заказчик\"},\"3\":{\"fn\":\"test1\",\"ft\":\"String\",\"fv\":\"1\",\"fdn\":\"fdn\"}},{\"0\":{\"fn\":\"maxPrice\",\"ft\":\"Price\",\"fv\":20325235.53,\"fdn\":\"Цена контракта\"},\"1\":{\"fn\":\"guaranteeApp\",\"ft\":\"Price\",\"fv\":35830945.54,\"fdn\":\"Обеспечение заявки\"},\"2\":{\"fn\":\"customer\",\"ft\":\"Object\",\"fv\":{\"guid\":\"\",\"name\":\"test2\"},\"fdn\":\"Заказчик\"},\"3\":{\"fn\":\"test1\",\"ft\":\"String\",\"fv\":\"1\",\"fdn\":\"fdn\"}}]}}"
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
