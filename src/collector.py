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

    def tender_list_gen(self, active):
        next_page = True
        page = 1
        while next_page is not None:
            tender_list_html_res = HttpWorker.get_tenders_list(active=active, page=page)
            parsing_type = 'ACTIVE' if active else 'ARCHIVE'
            next_page = Parser.next_page(tender_list_html_res.content)
            tender_list_gen = Parser.parse_tenders(tender_list_html_res.content, active)
            for tender_item in tender_list_gen:
                self.logger.info('[tender-{}] PARSING {} STARTED, URL:{}'.format(
                    tender_item['number'], parsing_type, tender_item['url']))
                res = self.repository.get_one(tender_item['id'])
                if res and res['status'] == tender_item['status']:
                    self.logger.info('[tender-{}] ALREADY EXIST, URL:{}'.format(
                        tender_item['number'], tender_item['url']))
                    continue
                tender_res = HttpWorker.get_tender(tender_item['url'])
                tender = Parser.parse_tender(tender_res.content, tender_item, active)
                lot_doc_description_html = HttpWorker.get_lot(tender['lot_url'])
                lot_doc_description = Parser.parse_lot(lot_doc_description_html.content)
                mapper = Mapper(id=tender['id'], number=tender['number'], status=tender['status'], http_worker=HttpWorker)
                mapper.load_tender_info(
                    tender['id'], tender['status'], tender['name'], tender['sub_start_date'], tender['sub_close_date'],
                    tender['tender_url'], lot_doc_description, tender['contacts'], tender['description'])
                mapper.load_customer_info(tender['customer'])
                yield mapper
                self.logger.info('[tender-{}] PARSING OK'.format(tender_item['number']))
            page += 1

    def db_send(self, active):
        for mapper in self.tender_list_gen(active):
            self.repository.upsert(mapper.tender_short_model)
            for model in mapper.tender_model_gen():
                self.rabbitmq.publish(model)

    def collect(self):
        while True:
            self.db_send(active=0)
            self.db_send(active=1)
            sleep(config.sleep_time)
