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
                    'displayName': li.text,
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
    def parse_tenders(cls, tenders_list_html_raw):
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        items = []
        for tender in html.find_all('div', {'class': 'line'})[:-1]:
            tender_rows = tender.find_all_next('p')
            items.append((
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
            'number': t_list_item[0],
            'name': t_list_item[1],
            'status': cls._get_status(t_list_item[3]),
            'region': 77,
            'attachments': cls._get_attachments(docs_block, t_list_item[3]),
            'customer': customer,
            'contacts': contacts,
        }

    @classmethod
    def parse_tender_gen(cls, tender_html_raw, dt_open):
        lots_gen = None
        tender_html = html.fromstring(tender_html_raw)
        lots_element = tender_html.xpath("//tr[@id='MainContent_carTabPage_TrLotPage2']")
        date_close_raw = tender_html.xpath("//span[@id='MainContent_carTabPage_txtBiddingEndDate']")
        price_raw = tender_html.xpath("//a[@id='MainContent_carTabPage_txtStartSumm']")
        price = float(price_raw[0].text.replace(',', '.').replace('\xa0', '')) if price_raw and price_raw[0].text \
            else None
        date_close = cls._parse_datetime_with_timezone(date_close_raw[0].text) if date_close_raw and date_close_raw[
            0].text else dt_open
        if date_close:
            status = 1 if date_close > tools.get_utc() else 3
        else:
            status = 3
        if lots_element:
            lots_trs = lots_element[0].xpath("td/table/tr[not(@class='DataGrid_HeaderStyle')]")
            lots_gen = cls._parse_lots_gen(lots_trs)
        yield status, price, date_close, lots_gen

    @classmethod
    def _parse_lots_gen(cls, lots_trs_elements):
        for lot_tr in lots_trs_elements:
            lot_tds = lot_tr.xpath("td")
            lot_num = int(lot_tds[0].text.strip().replace('\xa0', ''))
            lot_href_el = lot_tds[1].xpath("a")[0]
            lot_url = '%s/%s' % (config.base_url, lot_href_el.xpath("@href")[0])
            lot_name = lot_href_el.text.strip().replace('\xa0', '')
            lot_quantity = ('%s %s' % (lot_tds[3].text.strip(), lot_tds[2].text.strip())).replace('\xa0', '') if \
                lot_tds[2].text else lot_tds[3].text.strip().replace('\xa0', '')
            lot_price = float(lot_tds[4].text.replace(',', '.').replace('\xa0', ''))
            yield lot_num, lot_name, lot_url, lot_quantity, lot_price

    @classmethod
    def parse_lot_gen(cls, lot_html_raw):
        lot_html = html.fromstring(lot_html_raw)
        positions_trs = lot_html.xpath("//span[@id='MainContent_TableGround']/tr[@style='background:WhiteSmoke']")
        if positions_trs:
            pos_gen = cls._parse_positions_gen(positions_trs)
            yield pos_gen

    @classmethod
    def _parse_positions_gen(cls, positions_trs_list):
        for tr in positions_trs_list:
            spans = tr.xpath("td/span")
            name = spans[0].text.replace('\xa0', '')
            q, unit = None, None
            if len(spans) > 1:
                unit = spans[1].text.strip().replace('\xa0', '')
            if len(spans) > 2:
                q = spans[2].text.strip().replace('\xa0', '')
            quantity = '%s %s' % (q, unit) if unit and q else q if q else None
            yield name, quantity

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str):
        return tools.convert_datetime_str_to_timestamp(datetime_str + config.platform_timezone)
