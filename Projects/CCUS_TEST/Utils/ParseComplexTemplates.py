
import re
import pandas as pd


def parse_template(template_path, sheet_name, lower_headers_row_index=0, upper_headers_row_index=None,
                   data_content_column_index=None, column_name_separator=';'):
    """
    :param template_path: The template's path.
    :param sheet_name: The relevant template's sheet name.
    :param data_content_column_index: The first non-grid column's index (count starting at 0).
    :param upper_headers_row_index: The first (from top) headers-row's index (count starting at 0).
    :param lower_headers_row_index: The last (from top) headers-row's index (count starting at 0).
    :param column_name_separator: The separator used in between the different headers' names -
                                                                to form the new joint headers.
    :return: A data frame with a unified row of headers.
    """
    data = pd.read_excel(template_path, sheetname=sheet_name, skiprows=lower_headers_row_index)
    if data_content_column_index is not None and upper_headers_row_index is not None:
        new_columns = []
        for row_index in xrange(upper_headers_row_index, lower_headers_row_index + 1):
            header_data = pd.read_excel(template_path, sheetname=sheet_name, skiprows=row_index)
            columns = list(header_data.columns)[data_content_column_index:]
            if row_index != lower_headers_row_index:
                columns = pad_columns(columns)
            columns = get_real_columns(columns)
            if not new_columns:
                new_columns = list(columns)
            else:
                for i, column in enumerate(columns):
                    if column:
                        new_columns[i] += '{0}{1}'.format(column_name_separator, column)
        columns_to_rename = data.columns[data_content_column_index:]
        data = data.rename(columns={columns_to_rename[i]: new_columns[i] for i in xrange(len(new_columns))})
    for column in data[:data_content_column_index]:
        if 'Unnamed: ' in column:
            del data[column]
        strip_data_frame(data)
    return data.fillna('').astype(unicode)


def strip_data_frame(data):
    columns = data.columns
    for column in columns:
        if column.strip() != column:
            data = data.rename(columns={column: column.strip()})
    for column in data.columns:
        data[column] = data[column].apply(lambda x: x if not isinstance(x, (str, unicode)) else x.strip())
    return data


def get_real_columns(columns):
    for c in xrange(len(columns)):
        columns[c] = re.sub('\.\d+', '', columns[c])
        columns[c] = '' if 'Unnamed: ' in columns[c] else columns[c]
    return columns


def pad_columns(columns):
    previous_column = columns[0]
    for c in xrange(1, len(columns)):
        if 'Unnamed: ' in columns[c]:
            if 'Unnamed: ' not in previous_column:
                columns[c] = previous_column
        previous_column = columns[c]
    return columns
