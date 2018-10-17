from urllib.parse import urlencode

import requests

from src.config import config

if __name__ == '__main__':
    test_files_dir = '../test/files'
    with open('%s/%s' % (test_files_dir, 'customer_list.html'), 'w', encoding='utf-8') as f:
        f.write(requests.get(config.base_url).text)
    with open('%s/%s' % (test_files_dir, 'tenders_list.json'), 'w', encoding='utf-8') as f:
        query_dict = {'LOCATION_CODE': 'ОООШЛАКСЕРВИС'}
        f.write(requests.get('%s?%s' % (config.tenders_list_url, urlencode(query_dict, doseq=True))).text)
    with open('%s/%s' % (test_files_dir, 'positions.json'), 'w', encoding='utf-8') as f:
        query_dict = {'id': [2824998], 'did': 2824998}
        f.write(requests.get('%s?%s' % (config.tender_url, urlencode(query_dict, doseq=True))).text)
