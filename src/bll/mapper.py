import logging
from copy import copy

import src.bll.tools as tools
from sharedmodel.module import Root, Field, Customer
from sharedmodel.module.enum import FieldType, Modification
from sharedmodel.module.table import Head, Cell
from src.config import config


class Mapper:
    platform_name = 'Тендер.про'
    _platform_href = None
    _tender_short_model = None
    _customer_guid = None
    tender_id = None
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
    customer_name = None
    customer_region = None
    customer_inn = None
    customer_kpp = None
    tender_attachments = None
    tender_date_bidding = None
    tender_contacts = None
    tender_positions = None

    def __init__(self, number, status, sub_close_date, http_worker):
        """

        Args:
            id_(int): Ид тендера
            status(int): Статус тендера
            http_worker:
        """
        self.logger = logging.getLogger(
            '{}.{}'.format(config.app_id, 'mapper'))
        self.tender_id = number
        self.tender_status = status
        self.sub_close_date = sub_close_date
        self.http = http_worker

    @property
    def tender_short_model(self):
        if not self._tender_short_model:
            self._tender_short_model = {'_id': '{}_{}'.format(self.tender_id, 1), 'status': self.tender_status,
                                        'sub_close_date': self.sub_close_date}
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
        #if not lot and 'positions' in lot:
        if self.tender_positions:
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
                            Head(name='Quantity', displayName='Количество'),
                            Head(name='Measure', displayName='Ед.изм.')
                        ])
                    ).add_rows(
                        self.tender_positions,
                        lambda elem, row: row.add_cells([
                            Cell(
                                name='Name',
                                type=FieldType.String,
                                value=elem['name']
                            ),
                            Cell(
                                name='Quantity',
                                type=FieldType.String,
                                value=elem['quantity']
                            ),
                            Cell(
                                name='Measure',
                                type=FieldType.String,
                                value=elem['measure']
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
                type=FieldType.Date
            )).add_field(Field(
                name='SubmissionCloseDateTime',
                displayName='Дата окончания приема заявок',
                value=self.tender_date_open_until,
                type=FieldType.Date
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
            'number': self.tender_id,
            # Массив ОКПД (если присутствует) ex. ['11.11', '20.2']
            'okpd': [],
            # Массив ОКПД2 (если присутствует)
            'okpd2': [],
            # Массив ОКДП (если присутствует)
            'okdp': [],
            'ktru': [],
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
            'biddingDateTime': self.tender_date_bidding,
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
                    'orderName': self.tender_name,
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
                'orderName': self.tender_name,
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
            self._platform_href = 'http://www.tender.pro'
        return self._platform_href

    def load_customer_info(self, customer_name):
        org_info = self.http.get_organization(
            customer_name, self.customer_inn, self.customer_kpp)
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

    def load_tender_info(self, t_number, t_status, t_name, t_date_pub, t_date_close, t_url,
                         t_type, t_positions):
        self.tender_id = t_number
        self.tender_price = None
        self.tender_status = t_status
        self.tender_name = t_name
        self.tender_date_publication = t_date_pub
        self.tender_date_open = t_date_pub
        self.tender_date_open_until = t_date_close
        self.tender_url = t_url
        self.tender_lots = None
        self.tender_positions = t_positions
        self.tender_placing_way = config.placing_way[t_type]
        self.tender_placing_way_human = t_type
        self.tender_attachments = None
        self.tender_date_bidding = None
        self.tender_contacts = None
        return self