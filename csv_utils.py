import io
import csv


def find_encoding(file):
    import chardet

    rawdata = open(file, "r").read()
    result = chardet.detect(rawdata.encode())
    return result["encoding"]


def write_dict(csv_file, array_dict, delimiter=",", encoding=None):

    if array_dict == None or len(array_dict) < 1:
        return

    with io.open(csv_file, mode="w+") as file:
        header = array_dict[0].keys()
        writer = csv.DictWriter(
            file, fieldnames=header, quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        for index in range(1, len(array_dict)):
            writer.writerow(array_dict[index])


def write_rows(csv_file, rows, delimiter=",", encoding=None, append=False):

    if rows == None or len(rows) < 1:
        return

    mode = "a" if append else "w+"
    newline = "" if append else ""
    with io.open(csv_file, mode=mode, newline=newline) as file:
        writer = csv.writer(file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            if row[0].strip() != "":
                writer.writerow(row)
    file.close()


def read(
    csv_file,
    filter_columns=[],
    trim_space=True,
    delimiter=",",
    encoding=None,
    header_present=True,
):
    header = None
    enc = find_encoding(csv_file) if encoding == None else encoding
    with io.open(csv_file, encoding=enc) as file:
        reader = csv.reader(file, delimiter=delimiter)
        filtered = []
        error = False
        for row in reader:
            if error:
                break
            if filter_columns == None or len(filter_columns) < 1:
                filtered.append(
                    list(map(lambda e: e.strip() if trim_space else e, row))
                )
            else:
                filtered_row = []
                for index in filter_columns:
                    if index < len(row):
                        filtered_row.append(
                            row[index].strip() if trim_space else row[index]
                        )
                    else:
                        print(
                            "err: total column size: '{}' - provided invalid column index: '{}'".format(
                                len(row), index
                            )
                        )
                        error = True
                        break
                filtered.append(filtered_row)
        if len(filtered) > 0:
            if header_present:
                return filtered[:1][0], filtered[1:]
            else:
                return None, filtered


# test read
def read1(csv_file, filter_columns=[], trim_space=True, delimiter=","):
    header = None
    # enc = find_encoding(csv_file) if encoding == None else encoding

    with io.open(csv_file, encoding="utf-16", errors="ignore") as file:
        next(file)
        reader = csv.reader(file, delimiter=delimiter)
        filtered = []
        error = False
        for row in reader:
            if error:
                break
            if filter_columns == None or len(filter_columns) < 1:
                filtered.append(
                    list(map(lambda e: e.strip() if trim_space else e, row))
                )
            else:
                filtered_row = []
                for index in filter_columns:
                    if index < len(row):
                        filtered_row.append(
                            row[index].strip() if trim_space else row[index]
                        )
                    else:
                        print(
                            "err: total column size: '{}' - provided invalid column index: '{}'".format(
                                len(row), index
                            )
                        )
                        error = True
                        break
                filtered.append(filtered_row)
        if len(filtered) > 0:
            return filtered[:1][0], filtered[1:]


def convert_to_dict(header, data):

    created = []

    if header == None or len(header) < 1 or data == None or len(data) < 1:
        return created

    import collections

    if data != None and len(data) > 0:
        for row in data:
            obj = collections.OrderedDict()
            for index in range(0, len(header)):
                obj[header[index]] = row[index]
            created.append(obj)

    return created


def remove_duplicates(file=None, filter_columns=[], data=[], output_file=None):
    arr_of_dict = None
    if file != None:
        header, data = read(file, filter_columns=filter_columns)
        arr_of_dict = convert_to_dict(header, data)
    else:
        arr_of_dict = data
    itr = set()
    unique = []
    for elm in arr_of_dict:
        row = tuple(elm.items())
        if row not in itr:
            itr.add(row)
            unique.append(elm)
    if output_file != None:
        write_dict(output_file, unique)
    return unique
