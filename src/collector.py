import logging

from src.bll.http_worker import HttpWorker
from src.bll.mapper import Mapper
from src.bll.parser import Parser
from src.config import config
from src.repository.mongodb import MongoRepository
from src.repository.rabbitmq import RabbitMqProvider
from time import sleep


class Collector:
    __slots__ = ['logger', '_repository', '_rabbitmq', 'first_init']

    def __init__(self):
        self.logger = logging.getLogger('{}.{}'.format(config.app_id, 'collector'))
        self._repository = None
        self._rabbitmq = None
        self.first_init = True

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

    def tender_list_gen(self, arc=False):
        next_page_params = {
            'page': 1,
            'disp_status': 1 if arc else 0
        }
        while next_page_params is not None:
            #print('PAGE_PARAMS', next_page_params)
            tender_list_html_res = HttpWorker.get_tenders_list(next_page_params)
            next_page_exist, tender_list_gen = Parser.parse_tenders(tender_list_html_res.content,
                                                                    next_page_params['page'])
            #print(next_page_exist)
            for item in tender_list_gen:
                #print(item)
                tender_html_raw = HttpWorker.get_tender(item['url'])
                #tender_html_raw = HttpWorker.get_tender('https://b2b.sibur.ru/pages_new_ru/exchange/exchange_details.jsp?page=16&disp_status=0&id=319634&type=1')
                tender = Parser.parse_tender(tender_html_raw.content, item)
                self.logger.info('[tender-{}] PARSING STARTED'.format(tender['url']))
                res = self.repository.get_one(tender['number'] + '_1')
                if res and res['status'] == tender['status'] and res['mod_date'] == tender['mod_date']:
                    self.logger.info('[tender-{}] ALREADY EXIST'.format(tender['number']))
                    continue
                #print(tender)
                mapper = Mapper(number=tender['number'], status=tender['status'], mod_date=tender['mod_date'],
                                http_worker=HttpWorker)
                mapper.load_tender_info(tender['number'], tender['status'], tender['name'], tender['price'],
                                        tender['type'], tender['pub_date'], tender['sub_close_date'], tender['url'],
                                        tender['contacts'], tender['lots'], tender['dop_info'], tender['currency'])
                mapper.load_customer_info(tender['customer'])
                yield mapper
                self.logger.info('[tender-{}] PARSING OK'.format(tender['url']))
            if next_page_exist and self.first_init:
                next_page_params['page'] += 1
            elif arc and not self.first_init and next_page_params['page'] > config.max_archive_pages:
                next_page_params = None
            else:
                next_page_params = None

    def db_upload(self, arc):
        for mapper in self.tender_list_gen(arc=arc):
            self.repository.upsert(mapper.tender_short_model)
            #print(mapper.tender_short_model)
            for model in mapper.tender_model_gen():
                self.rabbitmq.publish(model)
                #print(model)

    def collect(self):
        while True:
            self.db_upload(arc=True)
            self.db_upload(arc=False)
            self.first_init = False
            sleep(config.sleep_time)
