
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
        data_columns = data.columns.tolist()[data_content_column_index:]
        columns_to_add = get_real_columns(data_columns)
        for header_row in xrange(upper_headers_row_index, lower_headers_row_index):
            header_data = pd.read_excel(template_path, sheetname=sheet_name, skiprows=header_row)
            header_columns = get_real_columns(header_data.columns.tolist()[data_content_column_index:])
            new_column_to_add = []
            for column_prefix in header_columns:
                for column_suffix in columns_to_add:
                    new_column_to_add.append('{}{}{}'.format(column_prefix, column_name_separator, column_suffix))
            columns_to_add = list(new_column_to_add)
        for column in columns_to_add:
            column_original_name = column.split(column_name_separator)[-1]
            data[column] = data[column_original_name]
        for column in data_columns:
            if column in data.columns:
                del data[column]
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


def get_real_columns(all_columns):
    columns = set()
    for column in all_columns:
        if 'Unnamed: ' not in column:
            column = re.sub('\.\d+', '', column)
            columns.add(column)
    return list(columns)


def pad_columns(columns):
    previous_column = columns[0]
    for c in xrange(1, len(columns)):
        if 'Unnamed: ' in columns[c]:
            if 'Unnamed: ' not in previous_column:
                columns[c] = previous_column
        previous_column = columns[c]
    return columns

