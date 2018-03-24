import os
import itertools

import tqdm
import elasticsearch
from .utils import csv_util


def _extract_to_csv(data, write_headers, fieldnames, output_folder,
                    worker_id):
    for key, group in itertools.groupby(data, key=lambda x: x['_index']):
        _rows = [_datumn['_source'] for _datumn in list(group)]
        output_file = _output_file_for(output_folder, worker_id, key)
        csv_util.write_dicts_to_csv(
            _rows, output_file, write_headers=write_headers,
            fieldnames=fieldnames[key])


def _output_file_for(output_folder, worker_id, index):
    return os.path.join(output_folder, '{}_{}.csv'.format(index, worker_id))


def _get_es_client(es_hosts):
    return elasticsearch.Elasticsearch(es_hosts)


def _add_slice_if_needed(total_worker_count, search_args, worker_id):
    if total_worker_count > 1:
        # Add ES scroll slicing to handle parallel scrolling of the same data
        search_args['body']['slice'] = {
            'id': worker_id,
            'max': total_worker_count
        }
    return search_args


def _get_data_from_es_page(_page):
    _data = _page['hits']['hits']
    _sid = _page['_scroll_id']
    return _data, _sid


def _init_progressbar(total_hits, worker_id):
    return tqdm.tqdm(total=total_hits, position=worker_id,
                     desc="slice-%s" % worker_id, ascii=True)


def scroll_and_extract_data(worker_id, total_worker_count, es_hosts,
                            es_timeout, search_args, fieldnames,
                            output_folder, progress_bar=True):
    _es = _get_es_client(es_hosts)
    search_args = _add_slice_if_needed(
        total_worker_count, search_args, worker_id)
    _page = _es.search(**search_args)
    _data, _sid = _get_data_from_es_page(_page)
    _total_hits = _page['hits']['total']
    _headers_written = False
    pbar = None
    if progress_bar:
        pbar = _init_progressbar(_total_hits, worker_id)
    if _data:
        _extract_to_csv(data=_data, output_folder=output_folder,
                        write_headers=(not _headers_written),
                        fieldnames=fieldnames, worker_id=worker_id)
        _headers_written = True
        if pbar:
            pbar.update(len(_data))
    while True:
        _page = _es.scroll(worker_id=_sid, scroll='{}m'.format(es_timeout))
        _data, _sid = _get_data_from_es_page(_page)
        if _data:
            _extract_to_csv(data=_data,
                            output_folder=output_folder,
                            write_headers=(not _headers_written),
                            fieldnames=fieldnames, worker_id=worker_id)
            _headers_written = True
            if pbar:
                pbar.update(len(_data))
        else:
            break


def get_fieldnames_for(es_hosts, indices):
    _es = _get_es_client(es_hosts)
    mappings = _es.indices.get_mapping(
        index=','.join(indices)
    )
    fieldnames = {}
    for index, value in mappings.items():
        index_mappings = value['mappings']
        for index_type, index_type_value in index_mappings.items():
            fieldnames[index] = list(index_type_value['properties'].keys())
            break
    return fieldnames
