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
    #REGEX_EMAIL = re.compile(r'mailto:[^\s]+@[^.]+.\w+')
    REGEX_EMAIL = re.compile(r'href=\"mailto:[^\"]+')
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
        html = BeautifulSoup(tenders_list_html_raw, 'lxml')
        pagination = html.find('div', {'id': 'pager'}).find_all('li')
        next_page = True if len(pagination) == 13 else False
        return next_page, cls._parse_tenders_gen(html)

    @classmethod
    def _parse_tenders_gen(cls, html):
        main_div = html.find('div', {'class': 'inputs2'}).find('div', {'class': 'ince'})
        odd = main_div.find_all('div', {'class': 'odd'})
        even = main_div.find_all('div', {'class': 'even'})
        tenders_html = odd + even
        tenders_list = []
        for item in tenders_html:
            date = item.find('div', {'class': 'fier'}).findAll(text=True)
            fiat = item.find('div', {'class': 'drei'}).findAll(text=True)
            tenders_list.append({
                'number': item.find('div', {'class': 'ein'}).text,
                'name': cls._clear_spec_letters(
                    item.find('div', {'class': 'zwei'}).find('div', {'class': 'ff_ie'}).text),
                'url': config.tender_url + item.find('div', {'class': 'zwei'}).find('a').attrs['href'],
                'org': item.find('div', {'class': 'zwei'}).find('div', {'class': 'org'}).find('em').text.replace('\t', ''),
                'price': cls._clear_spec_letters(fiat[0]),
                'currency': fiat[2],
                'pub_date': cls._clear_spec_letters(date[0]),
                'sub_close_date': date[2],
            })
        return tenders_list

    @classmethod
    def parse_tender(cls, text_html, item):
        html = BeautifulSoup(text_html, 'lxml')
        main_div = html.find('div', {'class': 'inputs2'})
        odd = main_div.find_all('div', {'class': 'odd'})
        even = main_div.find_all('div', {'class': 'even'})
        tender_info = odd + even
        lots = cls._find_tender_attr(tender_info, 'Предмет конкурентной процедуры:')
        contacts = cls._find_tender_attr(tender_info, 'Контактное лицо:')
        #print(contacts.findAll(text=True))
        last_mod = cls._find_tender_attr(tender_info, 'Дата последнего редактирования:').text
        pub_date = cls._parse_datetime_with_timezone(item['pub_date'], tz=False)
        last_mod_date = cls._parse_datetime_with_timezone(last_mod, tz=False)
        dop_info = cls._find_tender_attr(tender_info, 'Дополнительная информация:').text.strip()
        delivery_place = cls._find_tender_attr(
                tender_info, 'Адрес места поставки товара, проведения работ или оказания услуг:').text.strip()
        delivery_date = cls._find_tender_attr(tender_info, 'Сроки поставки (выполнения работ):').text.strip()

        return {
            'number': item['number'],
            'name': item['name'],
            'status': cls._get_status(item['sub_close_date']),
            'type': cls._clear_spec_letters(cls._find_tender_attr(tender_info, 'Вид процедуры:').findAll(text=True)[0]),
            'lots': cls._get_and_clear_lots(lots.find('p').findAll(text=True)),
            'price': item['price'],
            'currency': item['currency'],
            'delivery_date': None if delivery_date == '-' else delivery_date,
            'delivery_place': None if delivery_place == '-' else delivery_place,
            'pub_date': pub_date,
            'sub_close_date': cls._parse_datetime_with_timezone(item['sub_close_date'], tz=False),
            'customer': item['org'],
            'contacts': cls._get_contacts(contacts.findAll(text=True)),
            'last_mod': last_mod_date,
            'dop_info': None if dop_info == '-' else dop_info,
            'url': item['url']
        }

    @classmethod
    def _find_tender_attr(cls, attrs, search_str):
        for row in attrs:
            if row.find('div', {'class': 'first'}).text.strip() == search_str:
                return row.find('div', {'class': 'second'})

    @classmethod
    def _get_and_clear_lots(cls, lots_list):
        lots = lots_list[2::3]
        return [lot.replace('. ', '').strip() for lot in lots]

    @classmethod
    def _get_status(cls, close_date_str):
        current_date = tools.get_utc()
        close_date = cls._parse_datetime_with_timezone(close_date_str, tz=False)
        if current_date < close_date:
            return 1
        else:
            return 3

    @classmethod
    def _get_contacts(cls, text_lst):
        #full_text = ''.join(text_lst)
        fio_list = re.findall(cls.REGEX_FIO_PATTERNS, text_lst[1])
        email_list = re.findall(cls.REGEX_EMAIL, text_lst[1])
        #phone_list = re.findall(cls.REGEX_PHONE, text)
        contacts = []
        #print(fio_list)
        #print(email_list, text_lst)
        for fio, email in zip(fio_list, email_list):
            contacts.append({
                'fio': fio,
                'phone': cls._clear_spec_letters(text_lst[-1].replace(fio, '')),
                'email': email.replace('href="mailto:', '')
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
