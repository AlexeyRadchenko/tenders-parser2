import json
import logging
import os
import sys
from logging.config import dictConfig

from src.exceptions import ConfigException


class Config:
    statuses = {'None': 0, 'Active': 1, 'Commission': 2,
                'Closed': 3, 'Cancel': 4, 'Abandoned': 5}
    proxy = None

    def __init__(self):
        self.app_id = 'agro'
        self.root_dir = '%s/../..' % os.path.dirname(os.path.abspath(__file__))
        self.configure_logging()
        self.logger = logging.getLogger('{}.{}'.format(self.app_id, 'config'))
        file_config = self._read_config_file()
        # подключения
        self.mongo = file_config['mongodb']
        self.rabbitmq = file_config['rabbitmq']
        # соответствия
        self.placing_way = file_config['placing_way']
        self.customer_info_map = file_config['customer_info_map']
        self.platform_timezone = file_config['platform_timezone']
        # прокси
        if 'proxy' in file_config and file_config['proxy']['enabled']:
            self.set_up_proxy(file_config['proxy'])
        # ссылки
        self.base_url = 'http://agro.zakupki.tomsk.ru/Competition'
        self.tenders_list_url = '%s/%s' % (
            self.base_url, 'Competition_Request_Cost.aspx?Sale=0&AspxAutoDetectCookieSupport=1')
        self.tender_url = '%s/%s' % (self.base_url,
                                     'Competition_Document.aspx')
        self.lot_url = '%s/%s' % (self.base_url, 'Competition_lot_Pos.aspx')
        self.organizations_host = file_config["organizations"]["host"]
        self.organizations_token = file_config["organizations"]["token"]

    def set_up_proxy(self, proxy_config):
        proxy_str = "http://{host}".format(host=proxy_config['host'])
        # proxy_str = "http://{login}:{password}@{host}".format(
        #     login=proxy_config['login'],
        #     password=proxy_config['password'],
        #     host=proxy_config['host'])
        self.proxy = {"http": proxy_str, "https": proxy_str, "ftp": proxy_str}

    def configure_logging(self):
        log_dir = '%s/logs' % self.root_dir
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        dictConfig({
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'console': {
                    'format': '%(asctime)-15s %(levelname)s %(name)s %(message)s'
                },
            },
            'handlers': {
                'console': {
                    'level': logging.DEBUG,
                    'class': 'logging.StreamHandler',
                    'formatter': 'console'
                },
                'errors_file': {
                    'level': logging.WARNING,
                    'class': 'logging.FileHandler',
                    'formatter': 'console',
                    'filename': '%s/%s' % (log_dir, 'errors.log'),
                    'encoding': 'utf8',
                }
            },
            'loggers': {
                '%s' % self.app_id: {'handlers': ['console', 'errors_file']},
                '%s.http' % self.app_id: {},
                '%s.collector' % self.app_id: {},
                '%s.parser' % self.app_id: {},
                '%s.mapper' % self.app_id: {},
                '%s.config' % self.app_id: {},
            }
        })
        logging.getLogger(self.app_id).setLevel(logging.DEBUG)
        sys.excepthook = self.__handle_exception

    def _read_config_file(self):
        file_path = '%s/config.json' % self.root_dir
        try:
            with open(file_path, 'r', encoding='utf8') as cfg:
                return json.load(cfg)
        except Exception as e:
            raise ConfigException(
                'failed to load file `{}` with exception {}'.format(file_path, e))

    def __handle_exception(self, exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        logging.getLogger(self.app_id).critical(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


config = Config()

__all__ = ['config']
