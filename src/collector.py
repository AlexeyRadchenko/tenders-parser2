import logging
import datetime

from src.bll.http_worker import HttpWorker
from src.bll.mapper import Mapper
from src.bll.parser import Parser
from src.config import config
from src.repository.mongodb import MongoRepository
from src.repository.rabbitmq import RabbitMqProvider
from time import sleep


class Collector:
    __slots__ = ['logger', '_repository', '_rabbitmq', 'api_token']

    def __init__(self):
        self.logger = logging.getLogger('{}.{}'.format(config.app_id, 'collector'))
        self._repository = None
        self._rabbitmq = None
        self.api_token = None

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
    def _response_validate(cls, result, method):
        if result.get('error'):
            print('method: {} - {}'.format(method, result['error']['message']))
            return None
        else:
            return result

    @classmethod
    def _get_date_params(cls):
        now = datetime.datetime.now().timestamp()
        past = (datetime.datetime.now() - datetime.timedelta(days=7)).timestamp()
        return int(now), int(past)

    def tender_list_gen(self):
        now, past = self._get_date_params()
        tender_list_params = {
            'access_token': self.api_token,
            'date_from': past,
            'date_to': now,
        }

        tender_list_json_res = self._response_validate(HttpWorker.get_tenders_list(tender_list_params), 'tender_list')
        if not tender_list_json_res:
            yield tender_list_json_res
            raise StopIteration

        for tender_item in tender_list_json_res['trade_list']:
            t_params = {
                'access_token': self.api_token,
                'id': tender_item['id']
            }

            tender = self._response_validate(HttpWorker.get_tender(t_params), 'get_tender')

            if not tender:
                yield tender
                raise StopIteration
            tender = tender['short_trade_procedure']
            #print(tender['type'], tender['trade_type'])
            """if tender.get('positions'):
                #print(tender)
                
            else:
                continue"""

            self.logger.info('[tender-{}, URL: {}] PARSING STARTED'.format(tender['number'], tender['url']))
            status = Parser.get_status(tender['date_end'] * 1000, tender['status'])
            if status == 1:
                print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', tender)
            res = self.repository.get_one('{}_1'.format(tender['number']))
            if res and res['status'] == status and res['change_date'] == tender['change_date']:
                self.logger.info('[tender-{}] ALREADY EXIST'.format(tender['number']))
                continue
            mapper = Mapper(number=tender['number'], status=status, http_worker=HttpWorker,
                            change_date=tender['change_date'])
            mapper.load_tender_info(tender['number'], status, tender['description'], tender['publish_date'] * 1000,
                                    tender['date_end'] * 1000, tender['url'], tender['trade_type'], tender['lots'],
                                    tender.get('positions'), tender['date_begin'] * 1000, tender['comment'].replace('\\n', '\n'))
            mapper.load_customer_info(tender['customer']['name'])
            yield mapper
            self.logger.info('[tender-{}] PARSING OK'.format(tender['number']))

    def collect(self):
        if not self.api_token:
            from auth import AUTH
            result = self._response_validate(HttpWorker.get_api_token(AUTH), 'get_token')
            self.api_token = result.get('access_token')

        data = True
        print(self.api_token)
        while data:
            for mapper in self.tender_list_gen():
                if self.api_token and mapper:
                    self.repository.upsert(mapper.tender_short_model)
                    #print(mapper.tender_short_model)
                    for model in mapper.tender_model_gen():
                        self.rabbitmq.publish(model)
                        #print(model)
                elif not mapper:
                    data = False
            sleep(config.sleep_time)
