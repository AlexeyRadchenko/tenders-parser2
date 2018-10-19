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

    @classmethod
    def get_mapper_obj(cls, tender, status=None):
        mapper = Mapper(
            id_=tender['id'],
            status=status if status else tender['status'],
            url=tender['url'],
            pub_time=tender['pub_date'],
            sub_close_time=tender['sub_close_date'],
            http_worker=HttpWorker
        )
        mapper.load_tender_info(tender)
        mapper.load_customer_info(tender['customer'])
        return mapper

    def tender_result_list_gen(self):
        tender_result_list_res = HttpWorker.get_tenders_result_list(target_param=config.current_page)
        tender_result_list = Parser.parse_result_tenders(tender_result_list_res.content)
        for t_id, name, status, winner_or_reason in tender_result_list:
            self.logger.info('[tender-{}] PARSING ARCHIVE STARTED'.format(t_id))
            res = self.repository.get_one(t_id)
            if res and res['status'] == status:
                self.logger.info('[tender-{}] ALREADY EXIST'.format(res['url']))
                continue
            if res:
                self.logger.info('[tender-{}] PARSING STARTED'.format(res['url']))
                tender_html_res = HttpWorker.get_tender(res['url'])
                tender = Parser.parse_tender(
                    tender_html_res.content, (t_id, name, res['pub_date'], res['sub_close_date'], res['url'])
                )
                yield self.get_mapper_obj(tender, status)
                self.logger.info('[tender-{}] PARSING OK'.format(tender['url']))
            else:
                arc_short_model = Mapper.get_arc_short_model(t_id, status)
                self.repository.upsert(arc_short_model)
                yield None
                self.logger.info('[tender-{}] PARSING ARCHIVE OK'.format(t_id))

    def tender_list_gen(self):
        tender_list_html_res = HttpWorker.get_tenders_list()
        tender_list = Parser.parse_tenders(tender_list_html_res.content)
        for item in tender_list:
            self.logger.info('[tender-{}] PARSING STARTED'.format(item[5]))
            tender_html_res = HttpWorker.get_tender(item[5])
            tender = Parser.parse_tender(tender_html_res.content, item)
            res = self.repository.get_one(tender['id'])
            if res and res['status'] == 3:
                self.logger.info('[tender-{}] ALREADY EXIST'.format(tender['url']))
                continue
            yield self.get_mapper_obj(tender)
            self.logger.info('[tender-{}] PARSING OK'.format(tender['url']))

    def collect(self):
        while True:

            if self.first_init:
                pages_html_res = HttpWorker.get_pages_quantity()
                config.pages = Parser.get_pages_quantity(pages_html_res.content)

            for page in range(config.pages):
                for model in self.tender_result_list_gen():
                    if model:
                        self.repository.upsert(model.tender_short_model)
                        self.rabbitmq.publish(model)
                config.current_page['cpage'] += 1

            for mapper in self.tender_list_gen():
                self.repository.upsert(mapper.tender_short_model)
                for model in mapper.tender_model_gen():
                    self.rabbitmq.publish(model)

            if self.first_init:
                self.first_init = False
                config.pages = config.arc_page_count_after_first_time

            config.current_page['cpage'] = 1
            sleep(config.sleep_time)
