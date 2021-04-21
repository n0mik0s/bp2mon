import argparse
import os
import pprint
import yaml
import datetime
import bp2mon
import es
import multiprocessing as mp

def mp_wrapper(list_to_wrapper):
    _queue = list_to_wrapper[1]
    _dict = list_to_wrapper[0]
    _instance_name = _dict['instance_name']
    _object_name = _dict['object_name']
    _counter_name = _dict['counter_name']
    _metrics = _dict['metrics']
    _res = []

    _anomalies = bp2mon_session.anomalies(metrics=_metrics)
    if _anomalies:
        for _timestamp in _anomalies:
            _res.append({'instance_name': _instance_name,
                         'timestamp': _timestamp,
                         'object_name': _object_name,
                         'counter_name': _counter_name,
                         'counter_value': round(_anomalies[_timestamp], 2)})

        try:
            _queue.put(_res)
        except Exception as _err:
            print('ERR: [main:mp_wrapper] Failed to put _res in provided queue', _err)

if __name__=="__main__":
    startTime = datetime.datetime.now()
    pp = pprint.PrettyPrinter(indent=4)

    argparser = argparse.ArgumentParser(usage='%(prog)s [options]')
    argparser.add_argument('-c', '--conf',
                           help='Set full path to the configuration file.',
                           default='conf.yml')
    argparser.add_argument('-v', '--verbose',
                           help='Set verbose run to true.',
                           action='store_true')

    args = argparser.parse_args()

    verbose = args.verbose
    root_dir = os.path.dirname(os.path.realpath(__file__))
    conf_path_full = str(root_dir) + os.sep + str(args.conf)
    anomalies = []

    with open(conf_path_full, 'r') as reader:
        try:
            cf = yaml.safe_load(reader)
        except yaml.YAMLError as ex:
            print('ERR: [main]', ex)
            exit(1)
        else:
            if verbose:
                pp.pprint(cf)

            bp2mon_session = bp2mon.Bp2mon(es_config=cf['es_get'])
            metrics = bp2mon_session.metrics()
            anomalies = []
            dict_to_wrapper = {}
            list_of_dict_to_wrapper = []
            for instance_name in metrics:
                for object_name in metrics[instance_name]:
                    for counter_name in metrics[instance_name][object_name]:
                        metric = metrics[instance_name][object_name][counter_name]
                        if metric:
                            dict_to_wrapper = {'instance_name': instance_name,
                                               'object_name': object_name,
                                               'counter_name': counter_name,
                                               'metrics': metric}
                            list_of_dict_to_wrapper.append(dict_to_wrapper)

            proc_number = int(mp.cpu_count() - 2)
            manager = mp.Manager()
            queue = manager.Queue()
            with mp.Pool(proc_number) as pool:
                list_to_wrapper = list(zip([x for x in list_of_dict_to_wrapper],
                                           [queue for x in range(len(list_of_dict_to_wrapper))]))
                pool.map(mp_wrapper, list_to_wrapper)

            while not queue.empty():
                anomalies += queue.get()

            if anomalies:
                es_session = es.es(es_config=cf['es_put'])
                es_session.bulk_insert(es_config=cf['es_put'], js_arr=anomalies)

    print(datetime.datetime.now() - startTime)