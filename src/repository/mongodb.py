from pymongo import MongoClient


class MongoRepository:
    """
    Класс для работы с базой данных MongoDB
    """

    def __init__(self, host, port, database, collection):
        self.collection = MongoClient(host, port)[database][collection]

    def get_one(self, id_):
        """
        Получение одной записи из базы по id
        """
        result = self.collection.find_one({'_id': id_})
        return result

    def upsert(self, short_model):
        """Вставка/обновление записи в базе (update+insert=upsert)

        Args:
            short_model(dict):

        Returns:
            bool: Возвращает True если запись была обновлена либо создалась новая запись, False если запись присутствует
                в БД и в ней ничего не изменилось
        """
        a = self.collection.update_one({'_id': short_model['_id']}, {
            '$set': {'status': short_model['status'], 'change_date': short_model['change_date']}}, True)
        return True if a.modified_count or a.upserted_id else False
