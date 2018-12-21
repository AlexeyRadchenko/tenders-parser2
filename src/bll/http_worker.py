import logging

import requests

from src.bll.tools import retry
from src.config import config


class HttpWorker:
    timeout = 30
    logger = logging.getLogger('{}.{}'.format(config.app_id, 'http'))
    cookies = {'ASP.NET_SessionId': 'dkcoef1sbsslbuhcodmbdckf'}
    documentation_tab = {'__EVENTARGUMENT': 'CLICK:1'}
    addon_tab = {'__EVENTARGUMENT': 'CLICK:2'}

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
    def get_api_data(cls, url, target_param=None):
        res = requests.get(url, params=target_param, proxies=config.proxy)
        return res.json()
