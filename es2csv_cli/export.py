from elasticsearch import Elasticsearch
from .utils.csv_util import add_rows


def extract_to_csv(proc_name, data, es_columns, output_file):
    _rows = []
    for datumn in data:
        _row = []
        if '_all' in es_columns:
            for key, value in datumn['_source'].items():
                _row.append(value)
        else:
            for column in es_columns:
                _row.append(datumn['_source'].get(column))
        _rows.append(_row)
    add_rows(_rows, output_file)


def scroll_and_extract_data(proc_name, scroll_id, total_worker_count, es_hosts,
                            es_index, es_timeout, es_scroll_batch_size,
                            es_columns, output_file):
    _es = Elasticsearch(es_hosts)
    # Init scroll
    _es_search_body = {
        'query': {
            'match_all': {}
        }
    }
    if total_worker_count > 1:
        # Add ES scroll slicing to handle parallel scrolling of the same data
        _es_search_body['slice'] = {
            'id': scroll_id,
            'max': total_worker_count
        }

    _page = _es.search(
        index=es_index,
        scroll='{}m'.format(es_timeout),
        size=es_scroll_batch_size,
        body=_es_search_body
    )
    _data = _page['hits']['hits']
    _sid = _page['_scroll_id']
    if _data:
        extract_to_csv(proc_name, data=_data, es_columns=es_columns,
                       output_file=output_file)
    # Continue scrolling
    while True:
        _page = _es.scroll(scroll_id=_sid, scroll='{}m'.format(es_timeout))
        _sid = _page['_scroll_id']
        _data = _page['hits']['hits']
        if _data:
            extract_to_csv(proc_name, data=_data, es_columns=es_columns,
                           output_file=output_file)
        else:
            break
