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

    def tender_list_gen(self, arc):

        next_page_params = {
            'Organization': 1071,
            'customer': None,
            'Country': None,
            'Region': None,
            'SearchQuery': None,
            'Take': 100,
            'Tab': 1 if not arc else 4,
            'Skip': 0
        }

        while next_page_params is not None:
            tender_list_json_res = HttpWorker.get_tenders_list(next_page_params)
            if len(tender_list_json_res['Items']) == 0:
                break
            tender_list = Parser.parse_tenders(tender_list_json_res)
            for tender in tender_list:
                self.logger.info('[tender-{}] PARSING STARTED'.format(tender['number']))
                res = self.repository.get_one('{}_1'.format(tender['number']))
                if res and res['status'] == tender['status']:
                    self.logger.info('[tender-{}] ALREADY EXIST'.format(tender['number']))
                    continue
                mapper = Mapper(number=tender['number'], status=tender['status'], http_worker=HttpWorker)
                mapper.load_tender_info(tender['number'], tender['status'], tender['name'], tender['pub_date'],
                                        tender['sub_close_date'], tender['url'], tender['attachments'])
                mapper.load_customer_info(tender['customer'])
                yield mapper
                self.logger.info('[tender-{}] PARSING OK'.format(tender['number']))
            next_page_params['Skip'] += 100

    def db_upload(self, arc=False):
        for mapper in self.tender_list_gen(arc):
            self.repository.upsert(mapper.tender_short_model)
            for model in mapper.tender_model_gen():
                self.rabbitmq.publish(model)

    def collect(self):
        while True:
            self.db_upload(arc=True)
            self.db_upload()
            sleep(config.sleep_time)
