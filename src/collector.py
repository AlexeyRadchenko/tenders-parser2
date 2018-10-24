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
        for url, tenders_list_html_res in HttpWorker.get_tenders_list_gen(arc=arc):
            self.logger.info('[tenders_list-{}] PARSING STARTED'.format(url))
            tenders = Parser.parse_tenders(tenders_list_html_res.content, url, arc=arc)
            if not tenders:
                continue
            for tender in tenders:
                res = self.repository.get_one(tender['number'] + '_1')
                if res and res['status'] == tender['status']:
                    self.logger.info('[tender-{}] ALREADY EXIST, url:{}'.format(tender['number'], url))
                    continue
                mapper = Mapper(number=tender['number'], status=tender['status'], http_worker=HttpWorker)

                mapper.load_tender_info(
                    tender['number'], tender['status'], tender['name'], tender['pub_date'], tender['sub_close_date'],
                    url, tender['attachments'], tender['bidding_date'], tender['contacts'])
                mapper.load_customer_info('АО Кольская ГМК')
                yield mapper
                self.logger.info('[tender-{}, url:{}] PARSING OK'.format(tender['number'], url))

    def db_send(self, arc):
        for mapper in self.tender_list_gen(arc):
            self.repository.upsert(mapper.tender_short_model)
            for model in mapper.tender_model_gen():
                self.rabbitmq.publish(model)

    def collect(self):
        while True:
            self.db_send(arc=True)
            self.db_send(arc=False)
            sleep(config.sleep_time)

