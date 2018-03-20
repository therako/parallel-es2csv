import itertools

from tqdm import tqdm
from elasticsearch import Elasticsearch
from .utils.csv_util import write_dicts_to_csv


def extract_to_csv(proc_name, data, write_headers, fieldnames, output_prefix,
                   scroll_id):
    for key, group in itertools.groupby(data, key=lambda x: x['_index']):
        _rows = [_datumn['_source'] for _datumn in list(group)]
        output_file = _output_file_for(output_prefix, scroll_id, key)
        write_dicts_to_csv(_rows, output_file, write_headers=write_headers,
                           fieldnames=fieldnames[key])


def _output_file_for(output_prefix, scroll_id, index):
    return '{}{}_{}.csv'.format(output_prefix, index, scroll_id)


def scroll_and_extract_data(proc_name, scroll_id, total_worker_count, es_hosts,
                            es_timeout, search_args,
                            fieldnames, output_prefix):
    _es = Elasticsearch(es_hosts)
    # Init scroll
    if total_worker_count > 1:
        # Add ES scroll slicing to handle parallel scrolling of the same data
        search_args['body']['slice'] = {
            'id': scroll_id,
            'max': total_worker_count
        }
    _page = _es.search(**search_args)
    _data = _page['hits']['hits']
    _sid = _page['_scroll_id']
    _headers_written = False
    with tqdm(total=_page['hits']['total'], position=scroll_id,
              desc="slice-%s" % scroll_id, ascii=True) as pbar:
        if _data:
            extract_to_csv(proc_name, data=_data, output_prefix=output_prefix,
                           write_headers=(not _headers_written),
                           fieldnames=fieldnames, scroll_id=scroll_id)
            _headers_written = True
            pbar.update(len(_data))
        # Continue scrolling
        while True:
            _page = _es.scroll(scroll_id=_sid, scroll='{}m'.format(es_timeout))
            _sid = _page['_scroll_id']
            _data = _page['hits']['hits']
            if _data:
                extract_to_csv(proc_name, data=_data,
                               output_prefix=output_prefix,
                               write_headers=(not _headers_written),
                               fieldnames=fieldnames, scroll_id=scroll_id)
                _headers_written = True
                pbar.update(len(_data))
            else:
                break


def get_fieldnames_for(es_hosts, indices):
    _es = Elasticsearch(es_hosts)
    mappings = _es.indices.get_mapping(
        index=','.join(indices)
    )
    fieldnames = {}
    for index, value in mappings.items():
        index_mappings = value['mappings']
        for index_type, index_type_value in index_mappings.items():
            fieldnames[index] = index_type_value['properties'].keys()
            break
    return fieldnames
