# Shared Model Converter
DSL для создания общей модели тендера.

### API

Основные классы:
 - Root - обработчик дерева модели, валидация. Создание модели должно всегда начинаться с инициализации объекта этого класса.
 - Customer - содержит данные одного заказчика, имеет набор методов для указания цены, гарантий и названия заказчика.
 - Category - контейнер для Field, может также содержать Table или вложенный Category. Имеет собственное название, на клиенте выводится в виде "категории". Вложенности по возможности следует избегать. **Прим. "Порядок размещения заказа"**.
 - Field - содержит название поля, значение поля и его тип. Одна строка с данными из шаблона. **Прим. "Начало подачи заявок   25.05.2013 в 19:43"**.
 - table.Body - таблица, имеет собственное название, содержит заголовки таблицы и набор строк.
    - table.Row - класс для создания строки (заголовки также заносятся в строку).
    - table.Head - класс для создания заголовка.
    - table.Cell - ячейка таблицы, используется для создания строк.

Назначение функций каждого класса понятно из их названия.

*Примечание*: **add_general** исползуется для добавления Field в основную (верхнюю) категорию.

### Example

```python
Root() \
    .add_general() \
    .add_general() \
    .add_customer() \
    .add_category(
        lambda c: c
        .set_properties()
        .add_field()
        .add_field()
    ) \
    .add_category(
        lambda c: c
        .set_properties()
        .add_table(
            lambda ct: ct
            .set_properties()
            .set_header()
            .add_rows()
        )
    ) \
    .add_category(
        lambda c: c
        .set_properties()
        .add_field()
        .add_category(
            lambda cc: cc
            .set_properties()
            .add_field()
            .add_field()
        )
    )
``` 
### Example (Full)

```python
result = Root() \
    .add_general(
        Field(
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
```
