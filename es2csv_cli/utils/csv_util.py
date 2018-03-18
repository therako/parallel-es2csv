import re
import csv
import six


def add_rows(rows, filename, create_new=False, delimiter=',',
             quoting_policy=csv.QUOTE_ALL):
    mode = 'w+' if create_new else 'a+'
    with open(filename, mode) as csvfile:
        data_csv = csv.writer(
            csvfile, delimiter=delimiter, lineterminator='\n',
            quotechar='"', quoting=quoting_policy)
        for rowData in rows:
            row = []
            for columnData in rowData:
                if isinstance(columnData, six.string_types):
                    row.append(
                        re.sub(r"\s+", " ", columnData))
                else:
                    row.append(columnData)
            data_csv.writerow(row)
