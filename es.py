import elasticsearch
import pprint
import elasticsearch.helpers
import json
import query

class es():
    def __init__(self, es_config):
        self.es_config = es_config
        self.pp = pprint.PrettyPrinter(indent=4)

        try:
            if self.es_config['use_ssl']:
                self.es_eng = elasticsearch.Elasticsearch(
                    self.es_config['nodes'],
                    port=self.es_config['port'],
                    http_auth=(self.es_config['user'] + ':' + self.es_config['password']),
                    verify_certs=self.es_config['verify_certs'],
                    use_ssl=self.es_config['use_ssl'],
                    ca_certs=self.es_config['ca_cert']
                )
            else:
                self.es_eng = elasticsearch.Elasticsearch(
                    self.es_config['es_nodes'],
                    port=self.es_config['port'],
                    http_auth=(self.es_config['user'] + ':' + self.es_config['password'])
                )
        except Exception as _exc:
            print('ERR: [es:__init__]: Error with establishing connection with elastic cluster:', _exc)
            self.es_eng = False

    def bulk_insert(self, es_config, js_arr):
        _es_config = es_config
        _js_arr = js_arr
        _shards = _es_config['shards']
        _replicas = _es_config['replicas']
        _index = _es_config['index']
        _chunk_size = _es_config['chunk_size']

        _map = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date", "format": "yyyy-MM-dd' 'HH:mm:ss"},
                    "instance_name": {"type": "keyword"},
                    "object_name": {"type": "keyword"},
                    "counter_name": {"type": "keyword"},
                    "counter_value": {"type": "float"},
                }
            }
        }
        _body = {
            "settings": {
                "number_of_shards": _shards,
                "number_of_replicas": _replicas
            },
            "mappings": _map["mappings"]
        }
        _actions = [
            {
                "_index": _index,
                "_source": json.dumps(_js)
            }
            for _js in _js_arr
        ]

        if self.es_eng:
            if self.es_eng.indices.exists(index=_index):
                try:
                    self.es_eng.indices.delete(index=_index)
                except Exception as _err:
                    print('ERR: [es:bulk_insert]', _err)
                    return False

            try:
                self.es_eng.indices.create(index=_index, body=_body)
            except Exception as _err:
                print('ERR: [es:bulk_insert]', _err)
                return False

            try:
                elasticsearch.helpers.bulk(self.es_eng, _actions, chunk_size=_chunk_size, request_timeout=30)
            except Exception as _err:
                print('ERR: [es:bulk_insert]', _err)
                return False
            else:
                return True

    def search(self):
        _query = query.query
        _scroll = self.es_config['scroll']
        _size = self.es_config['size']
        _index = self.es_config['index']
        _request_timeout = self.es_config['request_timeout']

        try:
            _scan_res = elasticsearch.helpers.scan(client=self.es_eng,
                                                   request_timeout=_request_timeout,
                                                   query=_query,
                                                   scroll=_scroll,
                                                   size=_size,
                                                   index=_index,
                                                   clear_scroll=True,
                                                   raise_on_error=False)
        except Exception as _err:
            print('ERR: [es:search]', _err)
            return False
        else:
            _scan_res = list(_scan_res)

            return _scan_res