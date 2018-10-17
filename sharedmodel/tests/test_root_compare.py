import unittest
from copy import deepcopy

from module import Root, Category, Field
from module.table import Cell, Head
from module.enum import FieldType, Modification


class TestRootCompareMethods(unittest.TestCase):

    def test_compare_customers(self):
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

        result1 = Root() \
            .set_publication_date(1) \
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
        )

        result2 = Root() \
            .set_publication_date(2) \
            .add_customers(
                customers,
                lambda obj, c: c
                .set_properties(
                    max_price=obj['maxPrice'] + 50,
                    guarantee_app=obj['guaranteeApp'] + 50,
                    customer_name=obj['name'],
                    customer_guid=obj['guid']
                )
                .add_field(Field(
                    value="2",
                    name="test1",
                    displayName="fdn",
                    type=FieldType.String
                ))
        )

        result = result2.compare_many([result1])

        expected = "{\"general\":{\"customers\":[{\"0\":{\"fn\":\"maxPrice\",\"ft\":\"Price\",\"fv\":50325285.53,\"fdn\":\"Цена контракта\",\"ch\":{\"1\":50325235.53,\"2\":50325285.53}},\"1\":{\"fn\":\"guaranteeApp\",\"ft\":\"Price\",\"fv\":95830995.54,\"fdn\":\"Обеспечение заявки\",\"ch\":{\"1\":95830945.54,\"2\":95830995.54}},\"2\":{\"fn\":\"customer\",\"ft\":\"Object\",\"fv\":{\"guid\":\"\",\"name\":\"test1\"},\"fdn\":\"Заказчик\"},\"3\":{\"fn\":\"test1\",\"ft\":\"String\",\"fv\":\"2\",\"fdn\":\"fdn\",\"ch\":{\"1\":\"1\",\"2\":\"2\"}}},{\"0\":{\"fn\":\"maxPrice\",\"ft\":\"Price\",\"fv\":20325285.53,\"fdn\":\"Цена контракта\",\"ch\":{\"1\":20325235.53,\"2\":20325285.53}},\"1\":{\"fn\":\"guaranteeApp\",\"ft\":\"Price\",\"fv\":35830995.54,\"fdn\":\"Обеспечение заявки\",\"ch\":{\"1\":35830945.54,\"2\":35830995.54}},\"2\":{\"fn\":\"customer\",\"ft\":\"Object\",\"fv\":{\"guid\":\"\",\"name\":\"test2\"},\"fdn\":\"Заказчик\"},\"3\":{\"fn\":\"test1\",\"ft\":\"String\",\"fv\":\"2\",\"fdn\":\"fdn\",\"ch\":{\"1\":\"1\",\"2\":\"2\"}}}]}}"
        self.assertEqual(result.to_json(), expected)

    def test_compare_categories_with_general(self):
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

        obj1 = Root() \
            .set_publication_date(1) \
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
                    modifications=[Modification.CurEUR]
                ))
        )

        obj2 = deepcopy(obj1)

        obj2.set_publication_date(2)
        obj2.general[0].value += 50
        obj2.containers[0].entities[1].value = 'другая строка'
        obj2.containers[1].entities[0].value -= 50

        result = obj2.compare_many([obj1])

        expected = "{\"0\":{\"fn\":\"Category1\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Table1\",\"ft\":\"Table\",\"fv\":{\"th\":{\"0\":{\"fn\":\"Head1\",\"fv\":\"Header 1\"},\"1\":{\"fn\":\"Head2\",\"fv\":\"Header 2\"},\"2\":{\"fn\":\"Head3\",\"fv\":\"Header 3\"}},\"tb\":{\"0\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit1\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable1\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":1}},\"1\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit2\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable2\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":2}},\"2\":{\"0\":{\"fn\":\"Head1\",\"ft\":\"Integer\",\"fv\":\"SomeShit2\"},\"1\":{\"fn\":\"Head2\",\"ft\":\"String\",\"fv\":\"Nullable2\"},\"2\":{\"fn\":\"Head3\",\"ft\":\"String\",\"fv\":2}}}},\"fdn\":\"Таблица 1\"},\"1\":{\"fn\":\"Field1\",\"ft\":\"String\",\"fv\":\"другая строка\",\"fdn\":\"Поле категории 1\",\"md\":[\"CurEUR\"],\"ch\":{\"1\":\"просто строка\",\"2\":\"другая строка\"}}},\"fdn\":\"Категория 1\",\"md\":[\"Calendar\",\"Help\"]},\"1\":{\"fn\":\"Category2\",\"ft\":\"Category\",\"fv\":{\"0\":{\"fn\":\"Field2\",\"ft\":\"Price\",\"fv\":124074.54,\"fdn\":\"Поле категории 2\",\"md\":[\"CurEUR\"],\"ch\":{\"1\":124124.54,\"2\":124074.54}}},\"fdn\":\"Категория 2\",\"md\":[\"Calendar\",\"Help\"]},\"general\":{\"0\":{\"fn\":\"General1\",\"ft\":\"DateTime\",\"fv\":5039516,\"fdn\":\"Основное поле 1\",\"ch\":{\"1\":5039466,\"2\":5039516}},\"customers\":[]}}"
        self.assertEqual(result.to_json(), expected)


if __name__ == '__main__':
    unittest.main()
