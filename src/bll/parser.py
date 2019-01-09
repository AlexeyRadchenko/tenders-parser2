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
    def parse_tenders(cls, tenders_list_json):
        tender_list = []
        for item in tenders_list_json['Items']:
            sub_close_date = cls._parse_datetime_with_timezone(item['DateFinish'].split('T')[0], tz=None)
            pub_date = cls._parse_datetime_with_timezone(item['PublishingDate'].split('T')[0], tz=None)
            customer = item['Customers'][0]['Organization'] if item['Customers'] else None
            tender_list.append({
                'number': item['Number'],
                'name': item['Topic'],
                'sub_close_date': sub_close_date,
                'pub_date': pub_date,
                'org': item['OrganizerName'],
                'status': cls._get_status(item['IsCanceled'], item['IsFinished'], item['IsDisabled'], sub_close_date),
                'customer': customer['Name'] if customer else None,
                'region': config.lukoil_regions_id.get(str(customer['RegionId'])) if customer else None,
                'attachments': cls._get_attachments(item['Files'], pub_date),
                'url': 'www.lukoil.ru/Company/Tendersandauctions/Tenders?tab=1',
            })
        return tender_list

    @classmethod
    def _get_status(cls, cancel, finish, disable, end_date):
        if cancel:
            return 4
        if finish:
            return 3
        if disable:
            return 5
        if tools.get_utc() < end_date:
            return 1
        else:
            return 2

    @classmethod
    def _get_phone(cls, phone_str):
        phone = re.search(cls.REGEX_PHONE, phone_str)
        if phone:
            return phone.group()

    @classmethod
    def _get_attachments(cls, file_list, pub_date):
        attachments = []
        for file in file_list:
            attachments.append({
                'displayName': file['Title'],
                'href': file['FileDownloadUrl'],
                'publicationDateTime': pub_date,
                'realName': file['FileName'],
                'size': None,
            })
        return attachments

    @classmethod
    def _parse_datetime_with_timezone(cls, datetime_str, tz):
        if tz:
            return tools.convert_datetime_str_to_timestamp(datetime_str, config.platform_timezone, date_dilimiter='-')
        else:
            return tools.convert_datetime_str_to_timestamp(datetime_str, None, date_dilimiter='-')

    @classmethod
    def _clear_spec_letters(cls, string):
        return string.replace('\r', '').replace('\n', '').replace('\t', '').replace('\xa0', '').strip()
