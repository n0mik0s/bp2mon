import es
import datetime
import time
import pprint

from luminol.anomaly_detector import AnomalyDetector

class Bp2mon():
    def __init__(self, es_config):
        self.es_config = es_config
        self.pp = pprint.PrettyPrinter(indent=4)

    def metrics(self):
        _es_eng = es.es(es_config=self.es_config)
        _metrics = {}

        _scan_res = _es_eng.search()

        for _res in _scan_res:
            if '_source' in _res:
                _timestamp = _res['_source']['@timestamp']
                _cntr_value = _res['_source']['cntr_value']
                _counter_name = _res['_source']['counter_name']
                _instance_name = _res['_source']['instance_name']
                _object_name = _res['_source']['object_name']

                if _instance_name not in _metrics:
                    _metrics[_instance_name] = {}
                if _object_name not in _metrics[_instance_name]:
                    _metrics[_instance_name][_object_name] = {}
                if _counter_name not in _metrics[_instance_name][_object_name]:
                    _metrics[_instance_name][_object_name][_counter_name] = {}

                if _timestamp not in _metrics[_instance_name][_object_name][_counter_name]:
                    _metrics[_instance_name][_object_name][_counter_name][_timestamp] = _cntr_value
                else:
                    _metrics[_instance_name][_object_name][_counter_name][_timestamp] += _cntr_value

        return _metrics

    def anomalies(self, metrics):
        _metrics = metrics
        _metrics_for_luminol = {}
        _time_mapping = {}
        _anomalies = {}

        for _str in _metrics:
            _date = datetime.datetime.strptime(_str, "%Y-%m-%d %H:%M:%S")
            _time = datetime.datetime.timestamp(_date)
            _metrics_for_luminol[int(_time)] = _metrics[_str]
            _time_mapping[int(_time)] = _str

        if _metrics_for_luminol:
            _detector = AnomalyDetector(_metrics_for_luminol)
            _score = _detector.get_all_scores()

            if _score:
                for _timestamp, _value in _score.iteritems():
                    _anomalies[_time_mapping[_timestamp]] = _value
                return _anomalies
            else:
                return False
        else:
            return False