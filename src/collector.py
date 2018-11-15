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
        page = 1
        next_page = True
        while next_page is not None and page <= 20:
            tender_list_html_res = HttpWorker.get_tenders_list(page=page)
            print(config.tenders_list_url.format(page))
            next_page, tender_list = Parser.parse_tenders(tender_list_html_res.content)
            for tender_item in tender_list:
                #res = self.repository.get_one(tender_item['id'])
                #if res and res['status'] == 3:
                #    self.logger.info('[tender-{}] ALREADY EXIST'.format(t_url))
                #    continue
                self.logger.info('[tender-{}] PARSING STARTED'.format(tender_item['url']))
                tender_html_raw = HttpWorker.get_tender(tender_item['url'])
                tender = Parser.parse_tender(tender_html_raw.content, tender_item)
                print(tender)
                mapper = Mapper(t_id=tender['id'], status=tender['status'], http_worker=HttpWorker)

                mapper.load_tender_info(tender['status'], tender['name'], tender['pub_date'],
                                        tender['sub_close_date'], tender['url'],
                                        tender['attachments'], tender['contacts'], tender['dop_info'])
                mapper.load_customer_info(tender['customer'])
                self.logger.info('[tender-{}] PARSING OK'.format(tender['url']))
                yield mapper
            sleep(1)
            page += 1


    def collect(self):
        while True:
            for mapper in self.tender_list_gen():
                #self.repository.upsert(mapper.tender_short_model)
                print(mapper.tender_short_model)
                for model in mapper.tender_model_gen():
                    print(model)
                    #self.rabbitmq.publish(model)
            sleep(config.sleep_time)
