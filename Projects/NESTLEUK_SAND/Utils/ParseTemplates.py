import os
import re

import pandas as pd

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')


class NESTLEUK_SANDAvailabilityConsts(object):

    SHEET_NAME = 'BPPC_AVAILABILITY'

    KPI_NAME = 'KPI Name'
    PRODUCT_EAN_CODES = 'Products EANs'


class NESTLEUK_SANDParseTemplates:
    SHEET_NAME = 'KPIs'
    SEPARATOR = ', '
    SEPARATOR2 = ':'

    STORE_TYPE = 'Store Type'
    SEGMENTATION = 'Segmentation (Store.Additional_attribute_2)'
    EXECUTION_CONDITION = 'KPI Execution Condition (When no condition then Mandatory)'
    MANDATORY_SCENES = 'Mandatory Scenes'
    KPI_NAME = 'KPI name Eng'
    KPI_GROUP = 'KPI Group'
    SCENE_TYPE = 'Scene Types to Include'
    KPI_TYPE = 'KPI Type'
    SOS_ENTITY = 'SOVI Etitiy Type'
    SOS_NUMERATOR = 'SOVI Numerator'
    SOS_DENOMINATOR = 'SOVI Denominator'
    SURVEY_ID = 'Survey Q ID'
    SURVEY_ANSWER = 'Accepted Survey Answer'
    SURVEY_MODE = 'Multiple Survey Mode'
    TESTED_KPI_GRUOP = 'Tested KPI Group'
    TARGET = 'Target'
    SCORE = 'SCORE'
    WEIGHT = 'WEIGHT'
    GAP_PRIORITY = 'GAP PRIORITY'
    MANUFACTURER = 'Manufacturer'
    BRAND = 'Brand'
    CATEGORY = 'Category'
    SKU = 'SKU'
    SUM_OF_SCORES = 'Sum of scores'

    def __init__(self, template):
        self.template = template
        self.template_path = os.path.join(TEMPLATE_PATH, '{}.xlsx'.format(template))
        self.availability_consts = NESTLEUK_SANDAvailabilityConsts()

    def parse_template(self, template_path=None, sheet_name=None, lower_headers_row_index=0, upper_headers_row_index=None,
                       data_content_column_index=None, output_column_name_separator=';', input_column_name_separator=',',
                       columns_for_vertical_padding=[]):
        """
        :param template_path: The template's path.
        :param sheet_name: The relevant template's sheet name.
        :param data_content_column_index: The first non-grid column's index (count starting at 0).
        :param upper_headers_row_index: The first (from top) headers-row's index (count starting at 0).
        :param lower_headers_row_index: The last (from top) headers-row's index (count starting at 0).
        :param output_column_name_separator: The separator used in between the different headers' names -
                                                                    to form the new joint headers.
        :param input_column_name_separator: The separator of which the column names are joined by.
        :param columns_for_vertical_padding: Columns with merged cells to pad by the first value for each group.
        :return: A data frame with a unified row of headers.
        """
        data = pd.read_excel(self.template_path, sheetname=sheet_name, skiprows=lower_headers_row_index)
        if isinstance(data, dict):
            data = data.values()[0]
        if data_content_column_index is not None and upper_headers_row_index is not None:
            if output_column_name_separator == input_column_name_separator:
                print 'Input and output separators cannot be the same!'
                return data
            new_columns = []
            for row_index in xrange(upper_headers_row_index, lower_headers_row_index + 1):
                header_data = pd.read_excel(self.template_path, sheetname=sheet_name, skiprows=row_index)
                columns = list(header_data.columns)[data_content_column_index:]
                if row_index != lower_headers_row_index:
                    columns = self.pad_columns(columns)
                columns = self.get_real_columns(columns)
                if not new_columns:
                    new_columns = list(columns)
                else:
                    for i, column in enumerate(columns):
                        if column:
                            new_columns[i] += '{0}{1}'.format(output_column_name_separator, column)
            columns_to_rename = data.columns[data_content_column_index:]
            data = data.rename(columns={columns_to_rename[i]: new_columns[i] for i in xrange(len(new_columns))})
            columns_to_duplicate = {}
            for column in data.columns[data_content_column_index:]:
                if column and column.count(input_column_name_separator):
                    duplicates = []
                    for sub_column in column.split(output_column_name_separator):
                        if not duplicates:
                            duplicates = sub_column.split(input_column_name_separator)
                        else:
                            sub_duplicates = sub_column.split(input_column_name_separator)
                            new_duplicates = []
                            for sub_duplicate in sub_duplicates:
                                for duplicate in duplicates:
                                    new_duplicates.append('{0}{1}{2}'.format(duplicate, output_column_name_separator, sub_duplicate))
                            duplicates = list(new_duplicates)
                    columns_to_duplicate[column] = duplicates
            for column in columns_to_duplicate.keys():
                for duplicate in columns_to_duplicate[column]:
                    data[duplicate] = data[column]
                del data[column]
            for column in data[:data_content_column_index]:
                if 'Unnamed: ' in column:
                    del data[column]
        data = self.strip_data_frame(data)
        data = data.fillna('').astype(unicode)
        if columns_for_vertical_padding:
            self.pad_vertical_data(data, columns_for_vertical_padding)
        return data

    def strip_data_frame(self, data):
        columns = data.columns
        for column in columns:
            if column.strip() != column:
                data = data.rename(columns={column: column.strip()})
        for column in data.columns:
            data[column] = data[column].apply(lambda x: x if not isinstance(x, (str, unicode)) else x.strip())
        return data

    def get_real_columns(self, columns):
        for c in xrange(len(columns)):
            columns[c] = re.sub('\.\d+', '', columns[c])
            columns[c] = '' if 'Unnamed: ' in columns[c] else columns[c]
        return columns

    def pad_columns(self, columns):
        previous_column = columns[0]
        for c in xrange(1, len(columns)):
            if 'Unnamed: ' in columns[c]:
                if 'Unnamed: ' not in previous_column:
                    columns[c] = previous_column
            previous_column = columns[c]
        return columns

    def pad_vertical_data(self, data, columns):
        for column in columns:
            last_value = ''
            column_data = []
            for r in xrange(len(data)):
                row = data.iloc[r]
                if row[column]:
                    last_value = row[column]
                column_data.append(last_value)
            data[column] = column_data
        return data
