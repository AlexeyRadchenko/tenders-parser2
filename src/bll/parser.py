import logging
import re

from lxml import html

from src.bll import tools
from src.config import config
from bs4 import BeautifulSoup

class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_FIO_PATTERNS = re.compile(
        r'[А-Я][а-яё]+\s[А-Я][а-яё]+\s[А-Я][а-яё]+|[А-Я][а-яё]+\s[А-Я].[А-Я].|[А-ЯЁ].[А-ЯЁ].\s?[А-ЯЁ][а-яё]+')
    REGEX_EMAIL = re.compile(r'[^\s]+@[^.]+.\w+')
    REGEX_PHONE = re.compile(r'((8|\+7)[\- ]?)?(\(?\d{3,5}\)?[\- ]?)?[\d\- ]{7,16}')
    REGEX_FILE_NAME = re.compile(r"[^/]+$")

    @classmethod
    def _next_page_exist(cls, html):
        pagination = html.find('div', {'class': 'modern-page-navigation'}).find_all('a')
        if len(pagination) < 2:
            return None
        if html.find('div', {'class': 'modern-page-navigation'}).find_all('a')[-2].text == 'След.':
            return True

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw, *args):
        tenders_list_html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        next_page_exist = cls._next_page_exist(tenders_list_html)
        return next_page_exist, cls._parse_tenders_gen(tenders_list_html, *args)

    @classmethod
    def _parse_tenders_gen(cls, html, section, tender_type, page):
        tender_page_url = 'http://zakupki.sbu-azot.ru/procurement_requests/{0}/{1}/' \
                          '?SECTION_CODE=procurement_requests&SECTION_CODE_2={0}&SECTION_CODE_3={1}&PAGEN_1={2}'
        tender_items = html.find('div', {'class': 'item-page'}).find_all('div', {'class': 'warpPurc'})[:-1]
        items = []
        for tender in tender_items:
            number = tender.find('div', {'class': 'title'}).text
            pub_date = cls._parse_datetime_with_timezone(tender.find('div', {'class': 'date-ads'}).text, tz=True)
            items.append({
                'id': 'AZOT{}_1'.format(number),
                'number': 'AZOT{}'.format(number),
                'name': tender.find('div', {'class': 'subject'}).find('b').text,
                'status': cls._get_status(tender.find('div', {'class': 'textLine status'}).find('span').text),
                'type': tender.find('div', {'class': 'trades'}).text,
                'winner': tender.find('div', {'class': 'winner'}).text,
                'email': tender.find('div', {'class': 'mail'}).find('a').text,
                'pub_date': pub_date,
                'sub_close_date': cls._parse_datetime_with_timezone(
                    tender.find('div', {'class': 'date-open'}).text, tz=True),
                'customer': 'АО СДС Азот',
                'url': tender_page_url.format(section, tender_type, page)
            })
        return items

    @classmethod
    def _get_status(cls, status_str):
        if status_str == 'текущий':
            return 1
        if status_str == 'завершенный состоявшийся':
            return 3
        if status_str == 'завершенный несостоявшийся':
            return 5

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
