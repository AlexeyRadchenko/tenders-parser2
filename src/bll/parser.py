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
    REGEX_LOT_ROW = re.compile(r'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView_DXDataRow\d{1,5}')
    REGEX_DOC_ROW = re.compile(r'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_DocumentsGridView_DXDataRow\d{1,5}')

    @classmethod
    def get_tenders_list(cls, html_res):
        html = BeautifulSoup(html_res, 'lxml')
        table = html.find('table', {'id': 'ctl00_RootContentPlaceHolder_MainPageControl_ArchiveGridView_DXMainTable'})
        rows = [row for row in table.find_all('tr') if row.attrs.get('id') and re.search(
            'ctl00_RootContentPlaceHolder_MainPageControl_ArchiveGridView_DXDataRow\d{1,20}', row.attrs['id'])]
        tenders_list = []
        for row in rows:
            columns = row.find_all('td')
            tenders_list.append({
                'number': columns[0].find('a').text,
                'name': columns[1].find('a').text,
                'lots_quantity': int(columns[2].text),
                'start_date': columns[3].text,
                'end_date': columns[4].text,
                'type': columns[5].text,
                'status': columns[6].text,
                'link': columns[0].find('a').attrs['href'],
            })
        return tenders_list

    @classmethod
    def _get_tender_status(cls, status):
        if status in ['Объявлена',  'Прием ставок']:
            return 1
        elif status in ['Завершен прием ставок', 'Идет I этап', 'Завершен I этап', 'Идет II этап']:
            return 2
        elif status == 'Состоялась':
            return 3
        elif status in ['Не состоялась', 'Отменена']:
            return 4
        else:
            return 0

    @classmethod
    def parse_tender(cls, tender_html_raw, tender_item):
        html = BeautifulSoup(tender_html_raw, 'lxml')
        #print(html.find('span', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionLotsCountLabel'}).text)
        pub_date = cls._parse_datetime_with_timezone(html.find(
                'span', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_CDateLabel'}).text, False)
        print(tender_item['link'])
        return {
            'number': tender_item['number'],
            'name': tender_item['name'],
            'type': tender_item['type'],
            'status': cls._get_tender_status(tender_item['status']),
            'lots': cls._get_tender_lots(
                html.find('table', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_LotsGridView'})),
            'sub_start_date': cls._parse_datetime_with_timezone(tender_item['start_date'], False),
            'sub_close_date': cls._parse_datetime_with_timezone(tender_item['end_date'], False),
            'customer': html.find(
                'input', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_EnterpriseComboBox_I'}).attrs['value'],
            'contact_fio': html.find(
                'span', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionResponsiblePersonLabel'}).text,
            'pub_date': pub_date,
            'attachments': cls._get_attachments(html, pub_date, tender_item['link']),
        }

    @classmethod
    def _get_tender_lots(cls, lots_html):
        lots = [lot for lot in lots_html.find_all('tr')
                if lot.attrs.get('id') and re.search(cls.REGEX_LOT_ROW, lot.attrs['id'])]
        lots_list = []
        for lot_row in lots:
            lot_column = lot_row.find_all('td')
            lots_list.append({
                'name': lot_column[3].find('a').text,
                'url': lot_column[3].find('a').attrs['href'].replace('../', ''),
                'quantity': lot_column[4].text,
            })
        return lots_list

    @classmethod
    def parse_lot_info(cls, lot_html):
        lot_html = BeautifulSoup(lot_html, 'lxml')
        return {
            'measure': lot_html.find(
                'input', {'id': 'ctl00_RootContentPlaceHolder_LotFormLayout_UnitComboBox_I'}).attrs['value'],
            'price': float(lot_html.find(
                'input', {'id': 'ctl00_RootContentPlaceHolder_LotFormLayout_PlannedPricePerUnitTextBox_Raw'}
                ).attrs['value'].replace(',', '.')),
            'cost': float(lot_html.find(
                'input', {'id': 'ctl00_RootContentPlaceHolder_LotFormLayout_PlannedSumTextBox_Raw'}
                ).attrs['value'].replace(',', '.')),
        }

    @classmethod
    def _get_attachments(cls, doc_html, pub_date, tender_url):
        table = doc_html.find(
            'table', {'id': 'ctl00_RootContentPlaceHolder_AuctionFormLayout_AuctionPageControl_DocumentsGridView_DXMainTable'})
        doc_rows = [row for row in table.find_all('tr')
                    if row.attrs.get('id') and re.search(cls.REGEX_DOC_ROW, row.attrs['id'])]
        attachments = []
        for doc_row in doc_rows:
            column = doc_row.find_all('td')
            attachments.append({
                'displayName': column[0].text,
                'href': config.base_url + tender_url,
                'publicationDateTime': pub_date,
                'realName': None,
                'size': cls._get_file_size(column[2].text),
            })
        return attachments

    @classmethod
    def _get_file_size(cls, size_str):
        size_str = size_str.replace('\xa0', '')
        if 'КБ' in size_str:
            return int(float(size_str.rstrip(' КБ').replace(',', '.')) * 1000)
        if 'МБ' in size_str:
            return int(float(size_str.rstrip(' МБ').replace(',', '.')) * 1000000)
        if 'ГБ' in size_str:
            return int(float(size_str.rstrip(' ГБ').replace(',', '.')) * 1000000000)

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
