import logging

from src.bll.http_worker import HttpWorker
from src.bll.mapper import Mapper
from src.bll.parser import Parser
from src.config import config
from src.repository.mongodb import MongoRepository
from src.repository.rabbitmq import RabbitMqProvider
from time import sleep

class Collector:
    __slots__ = ['logger', '_repository', '_rabbitmq']

    def __init__(self):
        self.logger = logging.getLogger('{}.{}'.format(config.app_id, 'collector'))
        self._repository = None
        self._rabbitmq = None

    @property
    def repository(self):
        if not self._repository:
            self._repository = MongoRepository(config.mongo['host'], config.mongo['port'], config.mongo['database'],
                                               config.mongo['collection'])
        return self._repository

    @property
    def rabbitmq(self):
        if not self._rabbitmq:
            self._rabbitmq = RabbitMqProvider(config.rabbitmq['host'], config.rabbitmq['port'],
                                              config.rabbitmq['username'], config.rabbitmq['password'],
                                              config.rabbitmq['queue'])
        return self._rabbitmq

    def tender_list_gen(self):
        next_page_params = {
            "set_type_id": 2,
            "types": [1, 5, 2, 4, 6],
            "max_rows": 200,
            "_key": "1732ede4de680a0c93d81f01d7bac7d1",
            "set_id": 270, # 270
            "offset": 0  # + 200
        }
        while next_page_params is not None:
            tender_list_res = HttpWorker.get_api_data(config.tenders_list_url, next_page_params)
            data_length = len(tender_list_res['result']['data'])
            for item in tender_list_res['result']['data']:

                tender_params = {
                    "_key": "1732ede4de680a0c93d81f01d7bac7d1",
                    "company_id": item['company_id'],
                    "id": item['id']
                }

                tender = HttpWorker.get_api_data(config.tender_url, target_param=tender_params)

                if tender.get('result'):
                    tender_positions = HttpWorker.get_api_data(config.tender_positions_url, target_param=tender_params)
                    tender_company_info = HttpWorker.get_api_data(
                        config.tender_company_info, target_param={'id': item['company_id']})
                else:
                    continue
                self.logger.info('[tender-{}] PARSING STARTED'.format(tender['result']['data']['id']))
                tender_data = Parser.clear_tender_data(
                    tender['result']['data'],
                    tender_positions['result']['data'],
                    tender_company_info['result']['data'] if tender_company_info.get('result') else None,
                    item
                )

                res = self.repository.get_one(tender_data['id'])
                if res and res['status'] == tender_data['status'] and \
                        res['sub_close_date'] == tender_data['sub_close_date']:
                    self.logger.info('[tender-{}] ALREADY EXIST'.format(tender_data['id']))
                    continue
                mapper = Mapper(number=tender_data['number'], status=tender_data['status'],
                                sub_close_date=tender_data['sub_close_date'], http_worker=HttpWorker)
                mapper.load_tender_info(tender_data['number'], tender_data['status'], tender_data['name'],
                                        tender_data['pub_date'], tender_data['sub_close_date'], tender_data['url'],
                                        tender_data['type'], tender_data['positions'], tender_data['anno'],
                                        tender_data['currency'])

                mapper.load_customer_info(tender_data['customer'], tender_data['region'])
                yield mapper
                self.logger.info('[tender-{}] PARSING OK, URL-{}'.format(tender_data['id'], tender_data['url']))
            if data_length != 0:
                next_page_params['offset'] += 200
            else:
                next_page_params = None

    def collect(self):
        while True:
            for mapper in self.tender_list_gen():
                self.repository.upsert(mapper.tender_short_model)
                for model in mapper.tender_model_gen():
                    self.rabbitmq.publish(model)
            sleep(config.sleep_time)
