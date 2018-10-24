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
        next_page_params = {}
        while next_page_params is not None:
            tender_list_html_res = HttpWorker.get_tenders_list(next_page_params)
            next_page_params, tender_list_gen = Parser.parse_tenders(tender_list_html_res.text)
            for t_id, t_name, t_url, c_name, t_pway, t_pway_human, t_dt_publication, t_dt_open in tender_list_gen:
                if not c_name:
                    continue
                self.logger.info('[tender-{}] PARSING STARTED'.format(t_url))
                res = self.repository.get_one(t_id)
                if res and res['status'] == 3:
                    self.logger.info('[tender-{}] ALREADY EXIST'.format(t_url))
                    continue
                tender = {'id': t_id, 'name': t_name, 'customer_name': c_name, 'placing_way': t_pway,
                          'date_publication': t_dt_publication, 'date_open': t_dt_open, 'lots': [], 'date_close': None}
                tender_html_raw = HttpWorker.get_tender(tender_url=t_url)
                for t_status, t_price, t_dt_close, l_gen in Parser.parse_tender_gen(tender_html_raw.text, t_dt_open):
                    tender['date_close'] = t_dt_close
                    tender['status'] = t_status
                    mapper = Mapper(id_=tender['id'], status=tender['status'], http_worker=HttpWorker)
                    for l_num, l_name, l_url, l_quantity, l_price in l_gen:
                        lot = {'num': l_num, 'name': l_name, 'url': l_url, 'quantity': l_quantity, 'price': l_price,
                               'positions': []}
                        lot_html_raw = HttpWorker.get_lot(l_url)
                        for pos_gen in Parser.parse_lot_gen(lot_html_raw.text):
                            for p_name, p_quantity in pos_gen:
                                lot['positions'].append({'name': p_name, 'quantity': p_quantity})
                        tender['lots'].append(lot)
                    mapper.load_tender_info(t_id, t_status, t_name, t_price, t_pway, t_pway_human, t_dt_publication,
                                            t_dt_open, t_dt_close, t_url, tender['lots'])
                    mapper.load_customer_info(c_name)
                    yield mapper
                self.logger.info('[tender-{}] PARSING OK'.format(t_url))

    def collect(self):
        while True:
            for mapper in self.tender_list_gen():
                self.repository.upsert(mapper.tender_short_model)
                for model in mapper.tender_model_gen():
                    self.rabbitmq.publish(model)
            sleep(config.sleep_time)
