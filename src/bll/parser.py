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
    REGEX_NUMBER_FROM_URL = re.compile(r'[^\/]+\/?$')
    REGEX_CLOSE_DATE = re.compile(r'\d\d.\d\d.\d\d')
    REGEX_CLOSE_TIME = re.compile(r'\d\d:\d\d')

    @classmethod
    def _get_tender_number(cls, tender_num_url):
        number = re.search(cls.REGEX_NUMBER_FROM_URL, tender_num_url)
        if number:
            return number.group().rstrip('/')

    @classmethod
    def _next_page_exist(cls, html):
        if html.find('ul', {'class': 'pagination'}).find_all('li')[-1].text == 'След.':
            return True

    @classmethod
    def _get_sub_close_date_time(cls, datetime_str):
        date = re.search(cls.REGEX_CLOSE_DATE, datetime_str)
        if not date:
            return None
        time = re.search(cls.REGEX_CLOSE_TIME, datetime_str)
        short_year = date.group(0)[-2:]
        date = date.group(0)[:-2] + '20' + short_year

        if date and time:
            return '{} {}'.format(date, time.group())
        elif date:
            return date

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw):
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        return cls._next_page_exist(html), cls._parse_tenders(html)

    @classmethod
    def _parse_tenders(cls, html):
        table = html.find('div', {'class': 'zakupki-table'}).find('table').find('tbody')
        table_rows = table.find_all('tr')
        tenders_list = []
        for t_row in table_rows:
            columns = t_row.find_all('td')
            tenders_list.append({
                'pub_date': cls._get_sub_close_date_time(columns[0].text.replace('/', '.')),
                'sub_close_date': cls._get_sub_close_date_time(columns[1].text.replace('/', '.')),
                'name': columns[2].find('span', {'class': 'full-text'}).text,
                'url': config.base_url + columns[2].find('a').attrs['href'],
                'number': cls._get_tender_number(columns[2].find('a').attrs['href']),
                'customer': cls._clear_spec_letters(columns[3].text) if columns[3].text.strip() else cls._clear_spec_letters(
                    columns[4].text),
                'status': cls._get_status(columns[6].text),
            })
        return tenders_list

    @classmethod
    def parse_tender(cls, tender_html_raw, tender_item):
        html = BeautifulSoup(tender_html_raw, 'lxml')
        if tender_item['customer'] == 'ООО «Ксилотек-Сибирь»' or tender_item['customer'] == 'Лесосибирский ЛДК №1':
            tz = True
        else:
            tz = False
        pub_date = cls._parse_datetime_with_timezone(tender_item['pub_date'], False)
        tender_attrs = html.find_all('div', {'class': 'zakupki-form'})[1].find_all('div', {'class': 'col-sm-6'})
        dop_info = html.find('div', {'class': 'page-text'})
        tender_type = tender_attrs[3].find(
            'div', {'class': 'zf-item__name'}).text if len(tender_attrs) >= 4 else None
        if dop_info:
            dop_info = dop_info.text.replace('\xa0', '').replace('\t', '').replace('\n', '')

        return {
            'number': tender_item['number'],
            'name': html.find('h2', {'class': 'zakupki-heading'}).text,
            'status': tender_item['status'],
            'pub_date': pub_date,
            'sub_close_date': cls._parse_datetime_with_timezone(
                tender_item['sub_close_date'], tz) if tender_item['sub_close_date'] else None,
            'contacts': cls._get_contacts(tender_attrs),
            'attachments': cls._get_attachments(html.find('div', {'class':'files-items'}), pub_date),
            'type': tender_type if tender_type != 'Завершена' else 'открытый конкурс',
            'other_platform': cls._get_other_platform(html),
            'customer': tender_item['customer'],
            'dop_info':  dop_info,
            'url': tender_item['url'],
        }

    @classmethod
    def _get_other_platform(cls, html):
        if html.find('div', {'class': 'zakupki-etp'}):
            return True
        page_text = html.find('div', {'class': 'page-text'})
        if page_text:
            for link in page_text.find_all('a'):
                if 'mailto:' not in link.attrs['href']:
                    return True

    @classmethod
    def _get_status(cls, status_str):
        if not status_str:
            return 0
        if status_str == 'Приём заявок':
            return 1
        if status_str == 'Рассмотрение заявок':
            return 2
        if status_str == 'Завершена':
            return 3
        if status_str == 'Отменена':
            return 4
        if status_str == 'Приостановлена':
            return 5

    @classmethod
    def _get_contacts(cls, tenders_attrs):
        if len(tenders_attrs) < 3:
            return [{}]
        contacts = [{
            'fio': tenders_attrs[0].find('div', {'class': 'zf-item__name'}).text,
            'phone': tenders_attrs[1].find('div', {'class': 'zf-item__name'}).text,
            'email': tenders_attrs[2].find('div', {'class': 'zf-item__name'}).text
        }]
        return contacts

    @classmethod
    def _get_phone(cls, phone_str):
        phone = re.search(cls.REGEX_PHONE, phone_str)
        if phone:
            return phone.group()

    @classmethod
    def _get_attachments(cls, files_html, pub_date):
        if not files_html:
            return []
        file_divs = files_html.find_all('div', {'class': 'files-item'})
        attachments = []
        for file_item in file_divs:
            attachments.append({
                'displayName': file_item.find('div', {'class': 'files-item__title'}).text,
                'href': config.base_url + file_item.find('a').attrs['href'],
                'publicationDateTime': pub_date,
                'realName': cls._get_file_name(file_item.find('a').attrs['href']),
                'size': cls._get_file_size(file_item.find('div', {'class': 'files-item__text'}).text),
            })
        return attachments

    @classmethod
    def _get_file_name(cls, filename):
        filename = re.search(cls.REGEX_FILE_NAME, filename)
        if filename:
            return filename.group()
        else:
            return filename

    @classmethod
    def _get_file_size(cls, size_str):
        if 'kb' in size_str:
            return int(float(size_str.replace('kb', '').strip()) * 1000)
        if 'mb' in size_str:
            return int(float(size_str.replace('mb', '').strip()) * 1000000)
        if 'gb' in size_str:
            return int(float(size_str.replace('gb', '').strip()) * 1000000000)
        return float(size_str)

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
