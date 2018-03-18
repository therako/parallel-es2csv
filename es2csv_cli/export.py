from elasticsearch import Elasticsearch
from .utils.csv_util import write_dicts_to_csv


def extract_to_csv(proc_name, data, output_file, write_headers):
    _rows = []
    for _datumn in data:
        _row = _datumn['_source']
        _rows.append(_row)
    write_dicts_to_csv(_rows, output_file, write_headers=write_headers)


def scroll_and_extract_data(proc_name, scroll_id, total_worker_count, es_hosts,
                            es_timeout, output_file, search_args):
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
    if _data:
        extract_to_csv(proc_name, data=_data, output_file=output_file,
                       write_headers=(not _headers_written))
        _headers_written = True
    # Continue scrolling
    while True:
        _page = _es.scroll(scroll_id=_sid, scroll='{}m'.format(es_timeout))
        _sid = _page['_scroll_id']
        _data = _page['hits']['hits']
        if _data:
            extract_to_csv(proc_name, data=_data, output_file=output_file,
                           write_headers=(not _headers_written))
            _headers_written = True
        else:
            break
