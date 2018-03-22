import csv
from .file import purge


def write_dicts_to_csv(dicts, filename, delimiter=',',
                       fieldnames=[], write_headers=True):
    if len(dicts) > 0:
        if write_headers:
            # To make sure writting headers to the top of the file.
            purge(filename)
        with open(filename, 'a+') as output_file:
            if len(fieldnames) == 0:
                fieldnames = dicts[0].keys()
            csv_writer = csv.DictWriter(
                output_file, fieldnames=fieldnames, delimiter=delimiter)
            if write_headers:
                csv_writer.writeheader()
            for row in dicts:
                line_dict_utf8 = {}
                for k, v in row.items():
                    line_dict_utf8[k] = v
                csv_writer.writerow(line_dict_utf8)
