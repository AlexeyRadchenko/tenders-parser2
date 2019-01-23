import logging
import re


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
    def get_status(cls, date_end, status):
        actual = tools.get_utc() < date_end
        if actual:
            return 1
        if not actual and status == 'archived':
            return 3
        if status == 'canceled':
            return 4
        return 0

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
