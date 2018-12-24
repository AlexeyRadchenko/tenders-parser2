import logging
import re

from lxml import html

from src.bll import tools
from src.config import config


class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_FIO_PATTERNS = re.compile(
        r'[А-Я][а-яё]+\s[А-Я][а-яё]+\s[А-Я][а-яё]+|[А-Я][а-яё]+\s[А-Я].[А-Я].|[А-ЯЁ].[А-ЯЁ].\s?[А-ЯЁ][а-яё]+')
    REGEX_EMAIL = re.compile(r'[^\s]+@[^.]+.\w+')
    REGEX_PHONE = re.compile(r'((8|\+7)[\- ]?)?(\(?\d{3,5}\)?[\- ]?)?[\d\- ]{7,16}')
    REGEX_FILE_NAME = re.compile(r"[^/]+$")
    REGEXT_REMOVE_TAG = re.compile(r'<.*?>')

    @classmethod
    def clear_tender_data(cls, tender, positions, company, item):
        return {
            'id': '{}_{}'.format(tender['id'], 1),
            'number': tender['id'],
            'name': tender['title'],
            'type': tender['type_name'],
            'status': cls._get_status(tender['status_id']),
            'region': cls._get_company_region(company['address']) if company else None,
            'customer': item['company_name'],
            'currency': item['currency_name'],
            'delivery_date': tender['ship_date'],
            'positions': [
                {'name': pos['name'], 'quantity': pos['amount'], 'measure': pos['unit_name']} for pos in positions],
            'pub_date': cls._parse_datetime_with_timezone(tender['open_date'], False) if tender['open_date'] else None,
            'sub_close_date': cls._parse_datetime_with_timezone(tender['close_date'], False) \
                if tender['close_date'] else None,
            'url': 'http://www.tender.pro/view_tender_public.shtml?tenderid={}&tab=common'.format(tender['id']),
            'anno': re.sub(cls.REGEXT_REMOVE_TAG, '', tender['anno']),
        }

    @classmethod
    def _get_status(cls, status_num):
        if status_num == 1:
            return 1
        if status_num == 2:
            return 2
        if status_num in [3, 4, 5]:
            return 3
        if status_num == 100:
            return 0

    @classmethod
    def _get_company_region(cls, address):
        for region in config.regions_map.keys():
            if region in address:
                return config.regions_map[region]
        return 0

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
