import logging
import re

from _sha256 import sha256
from bs4 import BeautifulSoup

from src.bll import tools
from src.config import config


class Parser:
    __slots__ = ['customers_list_html']

    logger = logging.getLogger('{}.{}'.format(config.app_id, 'parser'))
    REGEX_EVENT_TARGET = re.compile(r"\('(.*)',")
    REGEX_TENDER_ID = re.compile(r"CID=(.*)$")

    @classmethod
    def next_page(cls, tenders_list_html):
        tenders_list_html = BeautifulSoup(tenders_list_html, 'lxml')
        next_page_hidden = tenders_list_html.find('ul', {'class': 'yiiPager'}).select('li.next.hidden')
        if next_page_hidden:
            return None
        else:
            return True

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw, active):
        tenders_list_html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        tenders_table_html_element = tenders_list_html.find('div', {'id': 'content'}).find('table')
        return cls._parse_tenders_gen(tenders_table_html_element, active)

    @classmethod
    def _parse_tenders_gen(cls, tenders_table_html, active):
        tenders = tenders_table_html.find_all('tr', {'class': 'registerBox'})
        result_list = []
        for tender in tenders:
            columns = tender.find_all('td')
            number = columns[0].find('div').find('b').text.lstrip(' №')
            sub_start_end = columns[2].find('div')
            result_list.append({
                'id': cls._get_tender_id(number),
                'number': number,
                'name': columns[1].find('div').find('b').text,
                'customer': columns[0].find_all('div')[1].text,
                'sub_start_date': sub_start_end.findAll(text=True)[1],
                'sub_close_date': sub_start_end.find('div').findAll(text=True)[2],
                'url': config.tender_url.format(number),
                'status': 1 if active else 3,
            })
        return result_list

    @classmethod
    def _get_tender_id(cls, tender_num):
        return 'НА{}_1'.format(int(sha256(tender_num.encode('utf-8')).hexdigest(), 16) % 10 ** 8)

    @classmethod
    def parse_tender(cls, tender_html_raw, tender_item, active):
        html = BeautifulSoup(tender_html_raw, 'lxml')
        tender_tables = html.find('div', {'id': 'content'}).find_all('table')
        lot_table_columns = tender_tables[-1].find_all('tr')
        tender_description = tender_tables[0].find_all('tr')
        tender_contacts = tender_tables[2].find_all('tr')
        return {
            'id': tender_item['id'],
            'number': tender_item['number'],
            'status': 1 if active else 3,
            'name': lot_table_columns[0].find('td').find('a').text,
            'customer': tender_item['customer'],
            'description': tender_description[1].find_all('td')[1].find('span').text.lstrip('\n').strip(),
            'sub_start_date': cls._parse_datetime_with_timezone(tender_item['sub_start_date'], tz=False),
            'sub_close_date': cls._parse_datetime_with_timezone(tender_item['sub_close_date'], tz=True),
            'contacts': cls._get_contacts(tender_contacts),
            'lot_url': config.base_url + lot_table_columns[0].find('td').find('a').attrs['href'],
            'tender_url': tender_item['url']
        }

    @classmethod
    def _get_contacts(cls, contacts_rows):
        contacts = []
        for row in contacts_rows:
            contact = row.find_all('td')[1].find('span').findAll(text=True)
            contacts.append({
                'fio': contact[0].lstrip('\n').lstrip(),
                'phone': contact[1].replace('\n', '').replace('Телефон:', '').lstrip(),
                'email': contact[2].replace('\n', '').replace('E-mail:', '').lstrip(),
            })
        return contacts

    @classmethod
    def parse_lot(cls, lot_html_raw):
        html = BeautifulSoup(lot_html_raw, 'lxml')
        lot_tables = html.find('div', {'class': 'tab-content'}).find('div', {'class': 'active'}).find_all('table')
        lot_description = lot_tables[0].find_all('tr')
        lot_docs = lot_tables[1].find('tr').find_all('td')[1].find_all('a')
        return {
            'deliver_deadline': lot_description[1].find_all('td')[1].find('span').text.lstrip('\n').strip(),
            'payment_terms': lot_description[2].find_all('td')[1].find('span').text.lstrip('\n').strip(),
            'special_terms': lot_description[3].find_all('td')[1].find('span').text.lstrip('\n').strip(),
            'certs': lot_description[5].find_all('td')[1].find('span').text.lstrip('\n').strip(),
            'attachments': cls._get_attachments(lot_docs),
        }

    @classmethod
    def _get_attachments(cls, docs_url):
        attachments = []
        for a in docs_url:
            if a.attrs['href'] == '/':
                continue
            attachments.append({
                'displayName': a.text,
                'href': config.base_url + a.attrs['href'],
                'publicationDateTime': None,
                'realName': a.text,
                'size': None,
            })
        return attachments

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)
