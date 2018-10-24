import logging
import re

from lxml import html

from src.bll import tools
from src.config import config
from bs4 import BeautifulSoup

class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_FILE_NAME = re.compile(r'[^\/]+$')
    REGEX_SHORT_FIO = re.compile(r'[А-Я][а-я]+\s[А-Я].[А-Я].')
    REGEX_FULL_FIO = re.compile(r'[А-Я][а-я]+\s[А-Я][а-я]+\s[А-Я][а-я]+')
    REGEX_PHONE = re.compile(r'тел.[^,]+')
    REGEX_EMAIL = re.compile(r'\w+@kolagmk.ru')

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw, url, arc):
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        if not arc:
            tenders_rows = html.find('table', {'class': 'cols-9'})
        else:
            tenders_rows = html.find('table', {'class': 'cols-10'})
        tenders = []
        if not tenders_rows:
            return None
        for tender in tenders_rows.find('tbody').find_all('tr', ['odd', 'even']):
            if not tender.find('table', {'class': 'cols-4'}):
                continue
            org, name = cls._get_org_name(
                tender.find('td', {'class': 'views-field-field-jtype-type'}).findAll(text=True))
            tenders.append({
                'number': tender.find('td', {'class': 'views-field-title'}).text.lstrip('\n').strip().lstrip('Лот №'),
                'status': cls._get_status(
                    tender.find('td', {'class': 'views-field-field-lotstatus'}).find('img').attrs['src']),
                'name': name,
                'sub_close_date': cls._parse_datetime_with_timezone(
                    tender.find('td', {'class': 'views-field-field-lotstartdate'}).find('span').text, tz=False),
                'bidding_date': cls._parse_datetime_with_timezone(
                    tender.find('td', {'class': 'views-field-field-lottorgdate'}).find('span').text, tz=False),
                'org': org,
                'contacts': cls._get_contacts(tender.find('td', {'class': 'views-field-field-jtype-date'}).text),
                'attachments': cls._get_attachments(
                    tender.find('td', {'class': 'views-field-field-jtype-files'}).find_all('tr')),
                'href': url,
                'pub_date': None,
            })
        return tenders

    @classmethod
    def _get_status(cls, src_str):
        regex_status = re.search(cls.REGEX_FILE_NAME, src_str)
        if regex_status:
            status = regex_status.group()
        else:
            return None
        if 'lotopen.png' == status:
            return 1
        if 'lotclose.png' == status:
            return 3

    @classmethod
    def _get_org_name(cls, str_list):
        org = str_list[0].lstrip('\n').strip()
        name = str_list[3].lstrip('\n').strip()
        return org, name

    @classmethod
    def _get_attachments(cls, tr_list):
        attachments = []
        for row in tr_list:
            doc_link = row.find('a')
            if not doc_link:
                continue
            attachments.append({
                'displayName': doc_link.text,
                'href': doc_link.attrs['href'],
                'publicationDateTime': None,
                'realName': doc_link.attrs['title'] if doc_link.attrs.get('title') else cls._get_filename_from_url(
                    doc_link.attrs['href']),
                'size': cls._size_convert(row.find_all('td')[2].text)
            })
        return attachments

    @classmethod
    def _get_filename_from_url(cls, url):
        filename = re.search(cls.REGEX_FILE_NAME, url).group()
        return filename.split('.')[0]

    @classmethod
    def _size_convert(cls, size_str):
        if 'КБ' in size_str:
            return int(float(size_str.rstrip(' КБ').replace(',', '.')) * 1000)
        if 'МБ' in size_str:
            return int(float(size_str.rstrip(' МБ').replace(',', '.')) * 1000000)
        if 'ГБ' in size_str:
            return int(float(size_str.rstrip(' ГБ').replace(',', '.')) * 1000000000)

    @classmethod
    def _get_contacts(cls, contacts_str):
        short_fio_regex = re.search(cls.REGEX_SHORT_FIO, contacts_str)
        full_fio_regex = re.search(cls.REGEX_FULL_FIO, contacts_str)
        phone_regex = re.search(cls.REGEX_PHONE, contacts_str)
        email_regex = re.search(cls.REGEX_EMAIL, contacts_str)
        fio, phone, email = None, None, None
        if short_fio_regex:
            fio = short_fio_regex.group()
        elif full_fio_regex:
            fio = full_fio_regex.group()

        if phone_regex:
            phone = phone_regex.group().lstrip('тел.').strip()
        if email_regex:
            email = email_regex.group()

        return fio, phone, email if fio or phone or email else None

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str + config.platform_timezone, tz=tz)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, tz=tz)
