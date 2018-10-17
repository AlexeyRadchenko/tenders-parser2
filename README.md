**Конфигурирование**

Для конфигурирования используется config.json

**Зависимости**

```bash
pip install -r requirements.txt
```

**Работа с докер окружением**

Для проверки работы может использоваться docker-compose файл из папки `dev`

Для запуса только окружения (rabbitmq и mongodb):

```bash
docker-compose -f ./dev/docker-compose.yml up mongo rabbitmq
```

в режиме демона

```bash
docker-compose -f ./dev/docker-compose.yml up -d mongo rabbitmq
```

Для запуска и окружения и скрипта-коллектора:

```bash
docker-compose -f ./dev/docker-compose.yml up
```

в режиме демона

```bash
docker-compose -f ./dev/docker-compose.yml up -d
```

`mongodb` и `rabbitmq` доступны по стандартным портам

Для портов необходимо поправить строчки в ./dev/docker-compose.yml

Пример мы хотим что бы rabbitmq был доступен на 5673 порте и админка 15673 порте, и mongodb на 27018 порте
rabbitmq:

```bash
ports:
      - 127.0.0.1:5673:5672
      - 127.0.0.1:15672:15673
```

mongo:

```bash
ports:
      - 127.0.0.1:27017:27018
```

Также можно посмотреть потребление ресурсов скриптом:

```bash
docker stats dev_collector_1
```


**Логирование**

Логи ошибок пишуться в папку logs в докер окружении также будут доступны (прокинут том на папку на локальном комьютере)

Также можно посмотреть логи из докера в режиме реального времени:

```bash
docker logs -f dev_collector_1 --tail 100
```