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

    @classmethod
    def _get_tender_id(cls, tender_num):
        return 'НА{}_1'.format(int(sha256(tender_num.encode('utf-8')).hexdigest(), 16) % 10 ** 8)

    @classmethod
    def _next_page_exist(cls, html):
        if html.find('div', {'class': 'pagination'}).find_all('a')[-1].attrs['class'][0] == 'pagination__arr':
            return True

    @classmethod
    def parse_tenders(cls, tenders_list_html_raw):
        tenders_list_html = html.fromstring(tenders_list_html_raw)
        next_page_params = {}
        tenders_table_html_element = tenders_list_html.xpath("//table[@id='MainContent_dgProducts']")[0]
        paginator = tenders_table_html_element.xpath("tr").pop().xpath("td")[0]
        next_page_el = paginator.xpath("a[text()='>']")
        if next_page_el:
            next_page_params['__EVENTTARGET'] = cls.REGEX_EVENT_TARGET.search(next_page_el[0].xpath("@href")[0]).group(
                1)
        if next_page_params:
            form = tenders_list_html.xpath("//form")[0]
            next_page_params.update({
                '__VIEWSTATE': form.xpath("div/input[@id='__VIEWSTATE']")[0].value,
                '__EVENTVALIDATION': form.xpath("div/input[@id='__EVENTVALIDATION']")[0].value})
        return next_page_params or None, cls._parse_tenders_gen(tenders_table_html_element)

    @classmethod
    def _parse_tenders_gen(cls, products_table_html_element):
        product_trs = products_table_html_element.xpath("tr[contains(@class,'ltin')]")
        for product_tr in product_trs:
            product_tds = product_tr.xpath("td")
            href = product_tds[4].xpath("a")[0]
            tender_name = href.xpath("span")[0].text.strip()
            tender_url = '%s/%s' % (config.base_url, href.xpath("@href")[0])
            tender_id = cls.REGEX_TENDER_ID.search(tender_url).group(1)
            dt_publication = cls._parse_datetime_with_timezone(product_tds[3].text)
            dt_open_str = product_tds[5].text.replace('\r\n', '').strip()
            dt_open = cls._parse_datetime_with_timezone(dt_open_str) if dt_open_str else None
            customer_name = product_tds[6].xpath("a/span")[0].text
            try:
                placing_way = config.placing_way[product_tds[7].text.lower()]
                placing_way_human = product_tds[7].text.lower()
            except KeyError:
                placing_way, placing_way_human = None, None
                cls.logger.warning('unknown tender placing way `{}`'.format(product_tds[7].text.lower()))
            yield (
                tender_id, tender_name, tender_url, customer_name, placing_way, placing_way_human, dt_publication,
                dt_open)

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
    def _get_contacts(cls, text_items):
        full_text = ' '.join(text_items)
        full_text = ' '.join(full_text.split())
        phone_numbers_text_list = [number
                                   for number in full_text.split(',') if
                                   ('8' in number or '7' in number) and 'Факс' not in number]
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
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone)
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None)

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
