import datetime
import time
from functools import wraps

import pytz
from requests import Response

from src.exceptions import MaxRetriesExceeded, SkipOnErrorException


def retry(logger, attempts=3, delay=100, exceptions=None):
    """Декоратор повторитель

    Args:
        logger(logging.Logger): Обьект логгера
        attempts(int): Кол-во попыток
        delay(int|float): Пауза в сек.
        exceptions(list of Exception): Список исключений на которых повторять. Если возникает исключение, которого
            нет в списке то повтора не будет а сразу вылетит исключение

    Returns:
        typing.Callable:
    """

    def decor(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            exc = None
            for i in range(1, attempts + 1):
                try:
                    res = f(*args, **kwargs)
                    if isinstance(res,Response) and res.text is None:
                        time.sleep(delay)
                        continue
                except exceptions or (Exception,) as e:
                    exc = e
                    logger.warning('retrying on exception `%s` (%d/%d)', exc, i, attempts)
                    time.sleep(delay)
                    continue
                if isinstance(res, Response) and res.status_code == 404:
                    raise SkipOnErrorException('bad url: {url}'.format(url=res.url))
                return res
            raise MaxRetriesExceeded('max retries exceeded on exception %s (%d/%d)' % (exc, attempts, attempts))

        return wrapper

    return decor


def convert_datetime_str_to_timestamp(datetime_str):
    """Преобразует строку с датой в `unix timestamp` формат

    Args:
        datetime_str(str):

    Returns:
        int: unix timestamp
    """
    if ':' in datetime_str:
        date_format = "%d.%m.%Y %H:%M:%S%z" if datetime_str.count(':') == 2 else "%d.%m.%Y %H:%M%z"
    else:
        date_format = "%d.%m.%Y%z"
    return int(datetime.datetime.strptime(datetime_str, date_format).astimezone(pytz.utc).timestamp()) * 1000


def get_utc():
    return int(time.time() * 1000)
