import logging

import requests

from src.bll.tools import retry
from src.config import config


class HttpWorker:
    timeout = 30
    logger = logging.getLogger('{}.{}'.format(config.app_id, 'http'))
    cookies = None

    @classmethod
    @retry(logger)
    def get_organization(cls, name, inn, kpp):
        result = {
            'guid': None,
            'name': name,
            'region': None
        }
        if inn is None or kpp is None:
            return result

        url = 'http://{}/organization?inn={}&kpp={}&name={}'.format(
            config.organizations_host, inn, kpp, name)
        headers = {'Authorization': config.organizations_token}
        r = requests.get(url, headers=headers)
        if r is None or r.text is None or r.status_code != 200:
            return result
        return r.json()

    @classmethod
    @retry(logger)
    def get_tenders_list(cls, target_param=None):
        if not target_param:
            res = requests.get(config.tenders_list_url,
                               cookies=cls.cookies, proxies=config.proxy)
        else:
            res = requests.post(config.tenders_list_url, data=target_param,
                                cookies=cls.cookies, proxies=config.proxy)
        return res

    @classmethod
    def get_tenders_result_list(cls, target_param=None):
        return requests.get(config.tenders_result_list_url,
                            params=target_param,
                            cookies=cls.cookies, proxies=config.proxy)

    @classmethod
    def get_pages_quantity(cls):
        return requests.get(config.tenders_result_list_url,
                            cookies=cls.cookies, proxies=config.proxy)
    @classmethod
    @retry(logger)
    def get_tender(cls, tender_url):
        return requests.get(tender_url, cookies=cls.cookies, proxies=config.proxy)

    @classmethod
    @retry(logger)
    def get_lot(cls, lot_url):
        return requests.post(lot_url, cookies=cls.cookies, proxies=config.proxy)
