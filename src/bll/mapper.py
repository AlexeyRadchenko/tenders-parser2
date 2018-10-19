import logging
from copy import copy

import src.bll.tools as tools
from sharedmodel.module import Root, Field, Customer
from sharedmodel.module.enum import FieldType, Modification
from sharedmodel.module.table import Head, Cell
from src.config import config


class Mapper:
    platform_name = 'АО "ССК"'
    _platform_href = None
    _tender_short_model = None
    _customer_guid = None
    tender_id = None
    tender_number = None
    tender_name = None
    tender_price = None
    tender_url = None
    tender_date_publication = None
    tender_date_open_until = None
    tender_date_open = None
    tender_lots = None
    tender_status = None
    tender_placing_way = None
    tender_placing_way_human = None
    tender_attachments = None
    tender_contacts = None
    customer_name = None
    customer_region = None
    customer_inn = None
    customer_kpp = None

    def __init__(self, id_, status, url, pub_time, sub_close_time, http_worker):
        """

        Args:
            id_(int): Ид тендера
            status(int): Статус тендера
            http_worker:
        """
        self.logger = logging.getLogger(
            '{}.{}'.format(config.app_id, 'mapper'))
        self.tender_id = id_
        self.tender_status = status
        self.url = url
        self.tender_date_publication = pub_time
        self.tender_date_open_until = sub_close_time
        self.http = http_worker

    @property
    def tender_short_model(self):
        if not self._tender_short_model:
            self._tender_short_model = {
                '_id': self.tender_id,
                'status': self.tender_status,
                'url': self.url,
                'pub_date': self.tender_date_publication,
                'sub_close_date': self.tender_date_open_until
            }
        return self._tender_short_model

    def tender_model_gen(self):
        yield from self._map_gen(one=False if self.tender_lots else True)

    def get_shared_model(self, lot=None):
        shared_model = Root()
        # в данной блоке должны присутствовать maxPrice, guaranteeApp, guaranteeContract
        # блок заказчика
        shared_model.add_customer(
            Customer().set_properties(
                max_price=lot['price'] if lot else self.tender_price,
                guarantee_app=None,
                guarantee_contract=None,
                customer_guid=self.customer_guid,
                customer_name=self.customer_name
            ))
        # информация о закупках
        if lot and 'positions' in lot:
            shared_model.add_category(
                lambda c: c.set_properties(
                    name='ObjectInfo',
                    displayName='Информация о объекте закупки'
                ).add_table(
                    lambda t: t.set_properties(
                        name='Objects',
                        displayName='Объекты закупки'
                    ).set_header(
                        lambda th: th.add_cells([
                            Head(name='Name', displayName='Наименование'),
                            Head(name='Code', displayName='ОКПД2'),
                            Head(name='Quantity', displayName='Количество'),
                            Head(name='PricePerOne',
                                 displayName='Цена за единицу'),
                            Head(name='Price', displayName='Стоимость')
                        ])
                    ).add_rows(
                        lot['positions'],
                        lambda elem, row: row.add_cells([
                            Cell(
                                name='Name',
                                type=FieldType.String,
                                value=elem['name']
                            ),
                            Cell(
                                name='Code',
                                type=FieldType.String,
                                value=None
                            ),
                            Cell(
                                name='Quantity',
                                type=FieldType.Integer,
                                value=elem['quantity']
                            ),
                            Cell(
                                name='PricePerOne',
                                type=FieldType.Price,
                                value=None
                            ),
                            Cell(
                                name='Price',
                                type=FieldType.Price,
                                value=None
                            )
                        ])
                    )
                )
            )
        # блок данных о заказе (в основном даты и места)
        shared_model.add_category(
            lambda c: c.set_properties(
                name='ProcedureInfo',
                displayName='Порядок размещения заказа'
            ).add_field(Field(
                name='SubmissionStartDateTime',
                displayName='Дата начал приема заявок',
                value=self.tender_date_open,
                type=FieldType.DateTime
            )).add_field(Field(
                name='SubmissionCloseDateTime',
                displayName='Дата окончания приема заявок',
                value=self.tender_date_open_until,
                type=FieldType.DateTime
            ))
        )
        # блок контактов и данных об организаторе
        shared_model.add_category(
                lambda c: c.set_properties(
                    name='Contacts',
                    displayName='Контактная информация',
                    modifications=[]
                ).add_field(Field(
                    name='Organization',
                    displayName='Организация',
                    value=self.customer_name,
                    type=FieldType.String,
                    modifications=[]
                    )
                ).add_array(
                    lambda c: c.set_properties(
                        name='Contacts',
                        displayName='Контакты',
                        modifications=[Modification.HiddenLabel]
                    ).add_array_items(
                        self.tender_contacts,
                        lambda item, index: c.add_field(Field(
                                name='FIO' + str(index),
                                displayName='ФИО',
                                value=item.get('fio'),
                                type=FieldType.String,
                                modifications=[Modification.HiddenLabel]
                            )
                        ).add_field(Field(
                            name='Phone' + str(index),
                            displayName='Телефон',
                            value=item.get('phone'),
                            type=FieldType.String,
                            modifications=[]
                            )
                        ).add_field(Field(
                            name='Email' + str(index),
                            displayName='Электронная почта',
                            value=item.get('email'),
                            type=FieldType.String,
                            modifications=[Modification.Email]
                            )
                        )
                    )
                )
        )
        return shared_model.to_json()

    def _map_gen(self, one=False):
        model = {
            # заказчики
            'customers': [{'guid': None, 'name': self.customer_name, 'region': self.customer_region}],
            # обеспечение заявки
            'guaranteeApp': None,
            # обеспечение контракта
            'guaranteeContract': None,
            # ссылка на тендер (лот)
            'href': self.tender_url,
            # Тендер - 0, план-график - 1
            # На большинстве площадок только тендеры, поэтому оставить 0
            # Если модель - план график, то 1
            'kind': 0,
            # Номер тендера (как в id, только без лота)
            'number': self.tender_number,
            # Массив ОКПД (если присутствует) ex. ['11.11', '20.2']
            'okpd': [],
            # Массив ОКПД2 (если присутствует)
            'okpd2': [],
            # Массив ОКДП (если присутствует)
            'okdp': [],
            # Статус тендера
            'status': self.tender_status,
            # TODO добавить self.customer_guid - сейчас заглушка
            # Строка для поиска организаций, customers+participants (Название организации ИНН КПП GUID)
            'organisationsSearch': ' '.join(
                (self.customer_name, str(self.customer_inn) if self.customer_inn else '',
                 str(self.customer_kpp) if self.customer_kpp else '')),
            # Способ определеняи поставщика
            'placingWay': self.tender_placing_way,
            # Ссылка на площадку (ссылка, название)
            'platform': {'href': self.platform_href, 'name': self.platform_name},
            # Дата публикации тендера UNIX EPOCH (UTC)
            'publicationDateTime': self.tender_date_publication,
            # Регион тендера (если нет явного, берем по региону заказчика)
            'region': self.customer_region,
            # Дата начала подачи заявок UNIX EPOCH (UTC)
            'submissionCloseDateTime': self.tender_date_open_until,
            # Дата окончания подачи заявок UNIX EPOCH (UTC)
            'submissionStartDateTime': self.tender_date_open,
            # Дата проведения аукциона в электронной форме (если есть) UNIX EPOCH (UTC)
            'biddingDateTime': self.tender_date_open,
            # Дата маппинга модели в UNIX EPOCH (UTC) (milliseconds)
            'timestamp': tools.get_utc(),
            # Тип парсера (можно не менять)
            'type': 234,
            # Версия извещения
            # Если на площадке нет версии, то ставить 1
            'version': 1,
            # Прикрепленные документы (массив)
            'attachments': self.tender_attachments
        }
        if not one:
            for lot_num, lot in enumerate(self.tender_lots):
                lot_model = copy(model)
                lot_model.update({
                    'id': '{}_{}'.format(self.tender_id, lot_num + 1),
                    'globalSearch': ' '.join((self.tender_name, str(self.tender_id), lot['name'],
                                              *[p['name'] for p in lot['positions']], self.tender_placing_way_human)),
                    'json': self.get_shared_model(lot),
                    'maxPrice': lot['price'],
                    'multilot': len(self.tender_lots) > 1,
                    'tenderSearch': ' '.join(
                        (self.tender_name, str(self.tender_id), lot['name'], *[p['name'] for p in lot['positions']])),
                })
                yield lot_model
        else:
            model.update({
                'id': '{}_{}'.format(self.tender_id, 1),
                'globalSearch': ' '.join((self.tender_name, str(self.tender_id), self.tender_placing_way_human)),
                'json': self.get_shared_model(),
                'maxPrice': self.tender_price,
                'multilot': False,
                'tenderSearch': ' '.join((self.tender_name, str(self.tender_id))),
            })
            yield model

    @property
    def customer_guid(self):
        if not self._customer_guid:
            self._customer_guid = ''
        return self._customer_guid

    @property
    def platform_href(self):
        if not self._platform_href:
            self._platform_href = 'https://tp.sibserv.com'
        return self._platform_href

    def load_customer_info(self, customer_name):
        org_info = self.http.get_organization(
            config.customer_info_map[customer_name]['name'], self.customer_inn, self.customer_kpp)
        if org_info['name']:
            self.customer_name = org_info['name']
        else:
            self.customer_name = customer_name
        if org_info['region']:
            self.customer_region = org_info['region']
        else:
            try:
                self.customer_region = int(
                    str(config.customer_info_map[customer_name]['inn'])[:2])
            except KeyError:
                self.logger.error(
                    'can`t find customer {} in customer map'.format(customer_name))
                self.customer_region = None
                self.customer_inn = None
                self.customer_kpp = None
                return self
        self.customer_inn = str(config.customer_info_map[customer_name]['inn'])
        self.customer_kpp = str(config.customer_info_map[customer_name]['kpp'])
        return self

    def load_tender_info(self, tender):
        self.tender_id = tender['id']
        self.tender_number = tender['number']
        self.tender_price = None
        self.tender_status = tender['status']
        self.tender_name = tender['name']
        self.tender_url = tender['url']
        self.tender_lots = None
        self.tender_placing_way = config.placing_way['открытый конкурс']
        self.tender_placing_way_human = 'открытый конкурс'
        self.tender_attachments = tender['attachments']
        self.tender_contacts = tender['contacts']
        return self
