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

    def get_page_params(self, section=None, tender_type=None):
        if section and tender_type:
            url = '%s/%s/%s/' % (config.base_url, section, tender_type)
            return {
               'PAGEN_1': 1,
               'SECTION_CODE': 'procurement_requests',
               'SECTION_CODE_2': section,
               'SECTION_CODE_3': tender_type
            }, url

    def tender_list_gen(self, status=[]):
        for section in config.tender_section_list:
            for tender_type in config.tender_type_list:
                next_page_params, url = self.get_page_params(section, tender_type)
                while next_page_params is not None:
                    print('Страница: ', next_page_params['PAGEN_1'], url)
                    tender_list_html_res = HttpWorker.get_tenders_list(url, target_param=next_page_params)
                    next_page_exist, tender_list = Parser.parse_tenders(
                        tender_list_html_res.content, section, tender_type, next_page_params['PAGEN_1'])
                    if not tender_list:
                        break
                    print(tender_list[0]['url'])
                    for tender in tender_list:
                        #self.logger.info('[tender-{}] PARSING STARTED'.format(t_url))
                        #res = self.repository.get_one(t_id)
                        #if res and res['status'] == 3:
                        #    self.logger.info('[tender-{}] ALREADY EXIST'.format(t_url))
                        #    continue

                        mapper = Mapper(number=tender['number'], status=tender['status'], http_worker=HttpWorker)

                        mapper.load_tender_info(tender['number'], tender['status'], tender['name'], tender['pub_date'],
                                                    tender['sub_close_date'], tender['url'], tender['email'])
                        mapper.load_customer_info(tender['customer'])
                        #yield mapper
                        if tender['status'] not in status:
                            status.append(tender['status'])
                    if next_page_exist:
                        next_page_params['PAGEN_1'] += 1
                    else:
                        next_page_params = None
        yield status

    def collect(self):
        while True:
            for mapper in self.tender_list_gen():
                #self.repository.upsert(mapper.tender_short_model)
                #for model in mapper.tender_model_gen():
                #    self.rabbitmq.publish(model)
                print(mapper)
            sleep(config.sleep_time)
