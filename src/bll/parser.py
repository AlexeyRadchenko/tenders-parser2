import logging
import re

#from lxml import html
from bs4 import BeautifulSoup

from src.bll import tools
from src.config import config


class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_DATE = re.compile(r'\d\d.\d\d.\d\d\d\d')
    REGEX_TIME = re.compile(r'\(\d{1,2}:\d{1,2}')
    REGEX_FILE_NAME = re.compile(r'[^\/]+$')
    REGEX_CUSTOMER = re.compile(r'–\s[^,]+')
    REGEX_PHONE = re.compile(r'тел.:.+, e-mail')
    REGEX_RESULT_PAGES = re.compile(r'\d+')

    @classmethod
    def _clear_date_time(cls, date_str):
        date = re.search(cls.REGEX_DATE, date_str)
        time = re.search(cls.REGEX_TIME, date_str)
        if date and time:
            return '{} {}'.format(date.group(), time.group().lstrip('('))
        elif date:
            return date.group()

    @classmethod
    def _get_status(cls, date_time_str):
        tender_end_time = tools.convert_datetime_str_to_timestamp(date_time_str + config.platform_timezone)
        current_time = tools.get_utc()
        if current_time < tender_end_time:
            return 1
        else:
            return 3

    @classmethod
    def _get_attachments(cls, docs_ul, pub_time):
        attachments = []
        for li in docs_ul.find_all('li')[1:]:
            doc = li.find('a')
            if doc:
                attachments.append({
                    'displayName': li.text.split(':')[0],
                    'href': doc.attrs['href'],
                    'publicationDateTime': tools.convert_datetime_str_to_timestamp(pub_time + config.platform_timezone),
                    'realName': re.search(cls.REGEX_FILE_NAME, doc.attrs['href']).group(),
                    'size': None,
                })
        return attachments

    @classmethod
    def _get_customer_contacts(cls, contacts_block):
        li_items = contacts_block.find_all('li')
        customer = None
        contacts = []
        for li in li_items:
            phone = re.search(cls.REGEX_PHONE, li.text)
            if not customer:
                customer = re.search(cls.REGEX_CUSTOMER, li.text).group().lstrip('– ')
            contacts.append({
                'fio': li.find('b').text,
                'phone': phone.group().lstrip('тел.: ').rstrip(', e-mail') if phone else None,
                'email': li.find('a').text,
            })
        return customer, contacts

    @classmethod
    def get_pages_quantity(cls, html_data):
        html = BeautifulSoup(html_data, 'lxml')
        last_page = html.find('div', {'class': 'multip'}).find_all('a')[-1].attrs['href']
        pages = re.search(cls.REGEX_RESULT_PAGES, last_page).group()
        return int(pages)

    @classmethod
    def _get_result_tender_status(cls, result_text):
        if 'Победитель(и) лота:' in result_text:
            return 3, result_text.split(': ')[1].strip()
        elif 'Лот отменен, причина отмены:' in result_text:
            return 4, result_text.split(': ')[1].strip()
        else:
            return 3, None

    @classmethod
    def parse_result_tenders(cls, tenders_result_html):
        html = BeautifulSoup(tenders_result_html, 'lxml')
        items = []
        tenders_div = [html.find('div', {'class': 'item0'})] + html.find_all('div', {'class': 'item'})
        for tender in tenders_div:
            tender_rows = tender.find_all('p')
            status, winner_reason = cls._get_result_tender_status(tender_rows[3].text)
            items.append((
                tender_rows[0].text.lstrip('Лот №').strip() + '_1',
                tender_rows[1].text.strip(),
                status,
                winner_reason
            ))
        return items

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw):
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        items = []
        for tender in html.find_all('div', {'class': 'line'})[:-1]:
            tender_rows = tender.find_all_next('p')
            items.append((
                tender_rows[0].text.lstrip('Лот №').strip() + '_1',
                tender_rows[0].text.lstrip('Лот №').strip(),
                tender_rows[1].text.strip(),
                cls._clear_date_time(tender_rows[3].findAll(text=True)[0]),
                cls._clear_date_time(tender_rows[3].findAll(text=True)[1]),
                tender_rows[4].find('a').attrs['href']
            ))
        return items

    @classmethod
    def parse_tender(cls, tender_html, t_list_item):
        html = BeautifulSoup(tender_html, 'lxml')
        content = html.find('div', {'id': 'content'})
        contacts_block, docs_block = content.find_all('ul', {'class': 'lottth'})
        customer, contacts = cls._get_customer_contacts(contacts_block)
        return {
            'id': t_list_item[0] + '_1',
            'number': t_list_item[1],
            'name': t_list_item[2],
            'status': cls._get_status(t_list_item[3]),
            'region': 77,
            'attachments': cls._get_attachments(docs_block, t_list_item[3]),
            'customer': customer,
            'contacts': contacts,
            'pub_date': cls._parse_datetime_with_timezone(t_list_item[3]),
            'sub_close_date': cls._parse_datetime_with_timezone(t_list_item[4]),
            'url': t_list_item[5],
        }

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str):
        return tools.convert_datetime_str_to_timestamp(datetime_str + config.platform_timezone)
