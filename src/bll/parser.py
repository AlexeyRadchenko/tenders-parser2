import logging
import re
from _sha256 import sha256

from lxml import html
from src.bll import tools
from src.config import config
from bs4 import BeautifulSoup


class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_FIO_PATTERNS = re.compile(r'[А-Я][а-яё]+\s[А-Я][а-яё]+\s[А-Я][а-яё]+|[А-Я][а-яё]+\s[А-Я].[А-Я].|[А-ЯЁ].[А-ЯЁ].\s?[А-ЯЁ][а-яё]+')
    REGEX_EMAIL = re.compile(r'[^\s]+@[^.]+.\w+')
    REGEX_PHONE = re.compile(r'((8|\+7)[\- ]?)?(\(?\d{3,5}\)?[\- ]?)?[\d\- ]{7,16}')
    REGEX_FILE_NAME = re.compile(r"[^/]+$")

    @classmethod
    def _get_tender_id(cls, tender_num):
        return 'КРУ{}_1'.format(int(sha256(tender_num.encode('utf-8')).hexdigest(), 16) % 10 ** 8)

    @classmethod
    def _next_page_exist(cls, html):
        if html.find('div', {'class': 'pagination'}).find_all('a')[-1].attrs['class'][0] == 'pagination__arr':
            return True

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw):
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        tenders_items_html = html.find('article', {'class': 'content'}).find_all('div', {'class': 'article-item'})
        return cls._next_page_exist(html), cls._parse_tenders_items(tenders_items_html)

    @classmethod
    def _parse_tenders_items(cls, tenders_list):
        items_list = []
        for tender in tenders_list:
            url_tag = tender.find('a')
            name = url_tag.find('span').text
            items_list.append({
                'id': cls._get_tender_id(name),
                'name': name,
                'url': config.base_url + url_tag.attrs['href'],
                'pub_date': tender.find('div', {'class': 'article-item__date'}).text.lstrip('\n').strip('\t')
            })
        return items_list

    @classmethod
    def parse_tender(cls, tender_html_raw, tender_item):
        html = BeautifulSoup(tender_html_raw, 'lxml')
        tender_data_table = html.find('article', {'class': 'content'}).find('table')
        header = tender_data_table.find('thead')
        if header:
            header_rows = header.find_all('tr')
            table_rows = tender_data_table.find('tbody').find_all('tr')
        else:
            tbody = tender_data_table.find('tbody')
            header_rows = tbody.find_all('tr')[:1]
            table_rows = tbody.find_all('tr')[1:]
        sub_end_date = cls._clear_spec_letters(table_rows[0].find_all('td')[1].text).rstrip('г.').strip()
        if sub_end_date[-3] == '.':
            sub_end_date = sub_end_date.replace('.18', '.2018')
        pub_date = cls._parse_datetime_with_timezone(tender_item['pub_date'], None)
        info = tender_data_table.find_next_siblings()
        contacts = table_rows[0].find_all('td')[-2].findAll(text=True)
        print(contacts)
        contacts = [item for item in contacts if item != '\n']
        if not contacts:
            contacts = [item.text for item in table_rows[0].find_all('td')[4].find_all('p')]
        if len(contacts) == 1 and 'Кол ед. оборудования' not in contacts[0]:
            contacts = table_rows[0].find_all('td')[4].findAll(text=True)
        return {
            'id': tender_item['id'],
            'name': ' '.join(tender_item['name'].split()),
            'status': cls._get_tender_status(sub_end_date),
            'region': 42,
            'customer': cls._get_customer(header_rows[0].find_all('td')[-2].findAll(text=True)),
            'pub_date': pub_date,
            'sub_close_date': cls._parse_datetime_with_timezone(sub_end_date, None),
            'contacts': cls._get_contacts(contacts),
            'attachments': cls._get_attachments(table_rows[0].find_all('td')[-1].find_all('a'), pub_date),
            'dop_info': cls._get_info(info),
            'url': tender_item['url']
        }

    @classmethod
    def _get_tender_status(cls, sub_end_date):
        sub_end_date_utc = cls._parse_datetime_with_timezone(sub_end_date, None)
        current_date = tools.get_utc()
        if current_date <= sub_end_date_utc:
            return 1
        else:
            return 3

    @classmethod
    def _get_customer(cls, column_text_list):

        for text in column_text_list:
            if 'Кузбассразрезуголь' in text:
                return 'АО «УК «Кузбассразрезуголь»'
            if 'АО' in text or 'ООО' in text or 'ЗАО' in text or 'ПАО' in text:
                return cls._clear_spec_letters(text)
        return 'ООО «СП «Серебряный ключ»'

    @classmethod
    def _get_contacts(cls, text_items):
        full_text = ' '.join(text_items)
        full_text = ' '.join(full_text.split())
        phone_numbers_text_list = [number
                                   for number in full_text.split(',') if ('8' in number or '7' in number) and 'Факс' not in number]
        fio_list = re.findall(cls.REGEX_FIO_PATTERNS, full_text)
        email_list = re.findall(cls.REGEX_EMAIL, full_text)
        phone_list = [cls._get_phone(item) for item in phone_numbers_text_list]
        contacts = []
        for fio, email, phone in zip(fio_list, email_list, phone_list):
            contacts.append({
                'fio': fio,
                'phone': phone,
                'email': email
            })
        return contacts

    @classmethod
    def _get_phone(cls, phone_str):
        phone = re.search(cls.REGEX_PHONE, phone_str)
        if phone:
            return phone.group()

    @classmethod
    def _get_attachments(cls, url_list, pub_date):
        attachments = []
        for url in url_list:
            attachments.append({
                'displayName': url.text.replace('\xa0', ''),
                'href': url.attrs['href'],
                'publicationDateTime': pub_date,
                'realName': cls._get_file_name(url.attrs['href']),
                'size': None,
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
    def _get_info(cls, info):
        if info:
            return ''.join([item.text for item in info]).replace('\xa0', '')

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
