import asyncio
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

    def tender_list_gen(self, arc=False):
        print('Запрос списка браузером')
        html_res = HttpWorker.get_tenders_list(arc=arc)
        tenders_list = Parser.get_tenders_list(html_res)
        for item in tenders_list:
            print(item['lots_quantity'] >= 100)
            if item['lots_quantity'] <= 100:
                print('Запрос тендера прогой')
                tender_html_raw = HttpWorker.get_tender(item['link'])
                tender = Parser.parse_tender(tender_html_raw.content, item)
            else:
                print('Запрос тендера браузером')
                tender_html_raw = HttpWorker.get_render_tender(item['link'])
                tender = Parser.parse_tender(tender_html_raw, item)
            for index, lot in enumerate(tender['lots']):
                print('Запрос запрос лота , браузером')
                lot_info_html = HttpWorker.get_lot(lot['url'])
                lot_info = Parser.parse_lot_info(lot_info_html)
                lot.update(lot_info)
                #res = self.repository.get_one(item['number'] + '_1')
                #if res and res['status'] == 3:
                #    self.logger.info('[tender-{}] ALREADY EXIST'.format(config.base_url))
                tender['lots'][index] = lot
            yield tender
            """mapper = Mapper(id_=tender['id'], status=tender['status'], http_worker=HttpWorker)
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
            self.logger.info('[tender-{}] PARSING OK'.format(t_url))"""

    def collect_tenders(self, arc=False):
        for mapper in self.tender_list_gen(arc=arc):
            print(mapper)
            #self.repository.upsert(mapper.tender_short_model)
            #for model in mapper.tender_model_gen():
            #    self.rabbitmq.publish(model)

    def collect(self):
        while True:
            self.collect_tenders(arc=True)
            self.collect_tenders()
            sleep(config.sleep_time)
