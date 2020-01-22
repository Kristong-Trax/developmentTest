# - *- coding: utf- 8 - *-
import os
import sys
import json
import pandas as pd
import xlsxwriter as xl

from Trax.Data.Orm.OrmCore import OrmSession


__author__ = 'sergey'


sys.path.append('.')

PROJECT = 'ccru_sand'

POS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data/KPIs_2020')
POS_PATH_VALIDATION = os.path.join(POS_PATH, 'POS_VALIDATION')
POS_PATH_VALIDATION_INPUT = os.path.join(POS_PATH, 'INPUT')
POS_PATH_VALIDATION_OUTPUT = os.path.join(POS_PATH, 'OUTPUT')
POS_LIST = {'file_name': 'PoS 2020 List.xlsx', 'sheet_name': 'PoS List'}
POS_ALL_FILE = 'PoS 2020 ALL.xlsx'
POS_KPIS_INTO_DB_OLD_FILE = 'PoS 2020 INTO DB kpis.xlsx'
POS_KPIS_INTO_DB_NEW_FILE = 'PoS 2020 INTO DB kpi_level_2.xlsx'

KPI_LEVEL_2_INSERT = "INSERT IGNORE INTO `static`.`kpi_level_2` (" \
                     "`type`, `client_name`, `kpi_family_fk`, `version`, " \
                     "`numerator_type_fk`, `denominator_type_fk`, `kpi_score_type_fk`, " \
                     "`kpi_result_type_fk`, `valid_from`, `valid_until`, `delete_time`, `initiated_by`, " \
                     "`context_type_fk`, `kpi_calculation_stage_fk`, `session_relevance`, `scene_relevance`, " \
                     "`planogram_relevance`, `live_session_relevance`, `live_scene_relevance`, " \
                     "`kpi_target_type_fk`, `is_percent`) VALUES "
KPI_LEVEL_2_VALUES = "('{}', '{}', 20, 1, " \
                     "999, NULL, NULL, " \
                     "NULL, NULL, NULL, NULL, 'Entity', " \
                     "NULL, 3, 0, 0, " \
                     "0, 0, 0, " \
                     "NULL, 0),"

MIN_KPI_NAME_LENGTH = 5

ERROR_INFO = 'ERROR - {} - {}'
WARNING_INFO = 'WARNING - {} - {}'

# POS columns list setting the columns order and MANDATORY_COLUMNS flag
POS_COLUMNS_LIST = [
    ['Sorting', False],
    ['PoS name', False],

    ['C_Group', False],
    ['C_Subgroup', False],
    ['L_Group', False],
    ['L_Subgroup', False],

    ['level', True],
    ['KPI ID', True],
    ['Parent', True],
    ['Children', True],

    ['KPI Weight', True],

    ['KPI name Eng', True],
    ['KPI name Rus', True],

    ['Category KPI Type', False],
    ['Category KPI Value', False],

    ['Formula', True],
    ['Logical Operator', False],

    ['Target', True],
    ['target_min', False],
    ['target_max', False],
    ['score_func', True],
    ['score_min', False],
    ['score_max', False],

    ['depends on', False],

    ['Type', True],
    ['Values', True],
    ['SKU', False],

    ['Products to exclude', False],
    ['Size', False],
    ['Form Factor', False],
    ['Sub brand', False],
    ['Brand', False],
    ['Manufacturer', False],
    ['Sub category', False],
    ['Product Category', False],

    ['shelf_number', False],
    ['Zone to include', False],
    ['Scenes to include', False],
    ['Scenes to exclude', False],
    ['Sub locations to include', False],
    ['Sub locations to exclude', False],
    ['Locations to exclude', False],
    ['Locations to include', False],

    ['Comments', False],

    ['SAP PoS', False],
    ['Channel', False],
    ['KPI Type', False],
    ['SAP KPI', False],
]
POS_COLUMNS = [x[0] for x in POS_COLUMNS_LIST]
MANDATORY_POS_COLUMNS = list(set([x[0] if x[1] else None for x in POS_COLUMNS_LIST]) - {None})

ALLOWED_SYMBOLS = {
    'KPI name Eng': u' '
                    u'0123456789'
                    u'ABCDEFGHIJKLMNOPQRSTUVWXWZ'
                    u'abcdefghijklmnopqrstuvwxyz'
                    u'%&()[]-_+/<>.:',
    'KPI name Rus': u' '
                    u'0123456789'
                    u'ABCDEFGHIJKLMNOPQRSTUVWXWZ'
                    u'abcdefghigklmnopqrstuvwxyz'
                    u'%&()[]-_+/<>.:'
                    u'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'
                    u'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
}

ALLOWED_FORMULAS_LIST = {
    'check_number_of_scenes_with_facings_target': {
        'levels': [3],
        'children': [],
        'depends_on':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'each SKU hits facings target': {
        'levels': [3],
        'children': [],
        'depends_on':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'facings TCCC/40': {
        'levels': [2, 3],
        'children': [],
        'depends_on': []
    },
    'Group': {
        'levels': [1],
        'children':
            ['facings TCCC/40',
             'number of atomic KPI Passed',
             'number of atomic KPI Passed on the same scene',
             'number of facings',
             'number of pure Coolers',
             'number of scenes',
             'number of SKU per Door RANGE',
             'number of SKU per Door RANGE TOTAL',
             'Scenes with no tagging',
             'Share of CCH doors which have 98% TCCC facings and no FC packs',
             'Share of CCH doors which have 98% TCCC facings',
             'SOS',
             'sum of atomic KPI result',
             'Weighted Average',
             'Group'],
        'depends_on': []
    },
    'Lead SKU': {
        'levels': [3, 4],
        'children': [],
        'depends_on': []
    },
    'number of atomic KPI Passed': {
        'levels': [2],
        'children':
            ['check_number_of_scenes_with_facings_target',
             'Lead SKU',
             'number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of facings',
             'number of pure Coolers',
             'number of scenes',
             'number of SKU per Door RANGE',
             'number of SKU per Door RANGE TOTAL',
             'number of sub atomic KPI Passed',
             'Scenes with no tagging',
             'SOS'],
        'depends_on': []
    },
    'number of atomic KPI Passed on the same scene': {
        'levels': [2],
        'children':
            ['Lead SKU',
             'number of facings',
             'number of scenes',
             'number of sub atomic KPI Passed'],
        'depends_on': []
    },
    'number of coolers with facings target and fullness target': {
        'levels': [3],
        'children': [],
        'depends_on': ['filled coolers target']
    },
    'number of doors of filled Coolers': {
        'levels': [3],
        'children': [],
        'depends_on': []
    },
    'number of doors with more than Target facings': {
        'levels': [3],
        'children': [],
        'depends_on':  ['filled coolers target']
    },
    'number of facings': {
        'levels': [2, 3, 4],
        'children': ['number of facings'],
        'depends_on':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'number of pure Coolers': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'number of scenes': {
        'levels': [2, 3],
        'children': [],
        'depends_on': []
    },
    'number of SKU per Door RANGE': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of coolers with facings target and fullness target',
             'filled coolers target']
    },
    'number of SKU per Door RANGE TOTAL': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of coolers with facings target and fullness target',
             'filled coolers target']
    },
    'number of sub atomic KPI Passed': {
        'levels': [3],
        'children':
            ['Lead SKU',
             'number of facings'],
        'depends_on': ['scene type']
    },
    'number of sub atomic KPI Passed on the same scene': {
        'levels': [3],
        'children':
            ['Lead SKU',
             'number of facings'],
        'depends_on': []
    },
    'Scenes with no tagging': {
        'levels': [2, 3],
        'children': [],
        'depends_on': []
    },
    'Share of CCH doors which have 98% TCCC facings and no FC packs': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'Share of CCH doors which have 98% TCCC facings': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'SOS': {
        'levels': [2, 3],
        'children': [],
        'depends_on':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
    'sum of atomic KPI result': {
        'levels': [2],
        'children':
            ['check_number_of_scenes_with_facings_target',
             'facings TCCC/40',
             'number of coolers with facings target and fullness target',
             'number of doors of filled Coolers',
             'number of doors with more than Target facings'],
        'depends_on': []
    },
    'Weighted Average': {
        'levels': [2],
        'children':
            ['check_number_of_scenes_with_facings_target',
             'each SKU hits facings target',
             'number of facings',
             'number of pure Coolers',
             'number of SKU per Door RANGE',
             'number of SKU per Door RANGE TOTAL',
             'number of sub atomic KPI Passed',
             'number of sub atomic KPI Passed on the same scene',
             'Scenes with no tagging',
             'Share of CCH doors which have 98% TCCC facings and no FC packs',
             'Share of CCH doors which have 98% TCCC facings',
             'SOS'],
        'depends_on':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target']
    },
}


def get_allowed_formulas_parameter(allowed_formulas_list, parameter):
    allowed_formulas_parameter = {}
    if parameter == 'parents':
        parent_to_any_children = []
        for parent in allowed_formulas_list.keys():
            for child in allowed_formulas_list[parent]['children']:
                if child == 'ANY':
                    parent_to_any_children += [parent]
                else:
                    if allowed_formulas_parameter.get(child) is None:
                        allowed_formulas_parameter[child] = [parent]
                    else:
                        allowed_formulas_parameter.update({child: allowed_formulas_parameter[child] + [parent]})
        if parent_to_any_children:
            for child in allowed_formulas_parameter.keys():
                allowed_formulas_parameter.update({child: allowed_formulas_parameter[child] + parent_to_any_children})
    else:
        for formula in allowed_formulas_list.keys():
            allowed_formulas_parameter[formula] = allowed_formulas_list[formula][parameter]
    return allowed_formulas_parameter


ALLOWED_FORMULAS = ALLOWED_FORMULAS_LIST.keys()
ALLOWED_FORMULAS_PARENTS = get_allowed_formulas_parameter(ALLOWED_FORMULAS_LIST, 'parents')
ALLOWED_FORMULAS_LEVELS = get_allowed_formulas_parameter(ALLOWED_FORMULAS_LIST, 'levels')
ALLOWED_FORMULAS_DEPENDENCIES = get_allowed_formulas_parameter(ALLOWED_FORMULAS_LIST, 'depends_on')

ALLOWED_CATEGORY_KPI_TYPES = [
    'Availability',
    'SOS'
]

ALLOWED_LOGICAL_OPERATORS = [
    'OR',
    'AND',
    'MAX',
    'SUM'
]

ALLOWED_LOGICAL_OPERATOR_KPIS = [
    'number of SKUs',
    'number of facings'
]

ALLOWED_SCORE_FUNCS = [
    'BINARY',
    'PROPORTIONAL',
    'CONDITIONAL PROPORTIONAL'
]

ALLOWED_VALUES_TYPES = [
    'MAN',
    'MAN in CAT',
    'CAT',
    'SUB_CATEGORY',
    'BRAND',
    'BRAND_IN_CAT',
    'SUB_BRAND',
    'SUB_BRAND_IN_CAT',
    'SKUs',
    'SCENES',
    'NUM_SCENES',
    'LOCATION_TYPE',
    'SUB_LOCATION_TYPE',
    'DOORS'
]

ADDITIONAL_DEPEND_ON_VALUES = ['scene type', 'filled coolers target']


class CCRU_SANDPOSValidator:

    def __init__(self):

        self.project = PROJECT

        self.rds_conn = OrmSession(self.project, writable=False)

        self.kpi_names = self.get_kpi_names()
        self.kpi_level_2_names = self.get_kpi_level_2_names()['name'].unique().tolist()
        self.manufacturers = self.get_manufacturers()['name'].unique().tolist()
        self.categories = self.get_categories()['name'].unique().tolist()
        self.sub_categories = self.get_sub_categories()['name'].unique().tolist()
        self.brands = self.get_brands()['name'].unique().tolist()
        self.sub_brands = self.get_sub_brands()['name'].unique().tolist()
        self.skus = self.get_skus()
        self.ean_codes = self.skus['ean_code'].unique().tolist()
        self.sizes = self.get_sizes()['size'].unique().tolist()
        self.form_factors = self.get_form_factors()['name'].unique().tolist()
        self.scene_types = self.get_scene_types()['name'].unique().tolist()
        self.sub_locations = self.get_sub_locations()['name'].unique().tolist()
        self.locations = self.get_locations()['name'].unique().tolist()
        self.store_zones = self.get_store_zones()['name'].unique().tolist()

    def validate_and_transform(self):

        pos_list_df = pd.read_excel(os.path.join(POS_PATH_VALIDATION, POS_LIST['file_name']),
                                    sheet_name=POS_LIST['sheet_name'], convert_float=True)
        pos_list_df = pos_list_df.where((pd.notnull(pos_list_df)), None)

        pos_all = pd.DataFrame(columns=POS_COLUMNS)
        for index, row in pos_list_df.iterrows():
            pos_name = row['POS']
            file_in = row['File_in']
            sheet_in = row['Sheet_in'] if row['Sheet_in'] else 0
            file_out = row['File_out']
            sheet_out = row['Sheet_out'] if row['Sheet_out'] else 'Sheet1'

            pos = pd.read_excel(os.path.join(POS_PATH_VALIDATION_INPUT, file_in),
                                sheet_name=sheet_in,
                                converters={
                                    'KPI ID': int,
                                    'Parent': int,
                                    'Children': str,
                                    'level': int
                                })
            pos = pos.where((pd.notnull(pos)), None)
            pos = pos.reset_index(drop=True)

            if 'PoS name' in pos.columns:
                pos = pos[pos['PoS name'] == pos_name]
            else:
                pos['PoS name'] = pos_name

            pos['00_File_out'] = file_out
            pos['00_Sheet_out'] = sheet_out

            # stripping all data from leading and trailing extra spaces
            columns = pos.columns
            for i, r in pos.iterrows():
                for c in columns:
                    if type(r[c]) in (str, unicode):
                        pos.loc[i, c] = r[c].strip()

            structure_ok = True
            contents_ok = True

            # checking mandatory columns
            error_name = 'COLUMN IS NOT FOUND'
            for c in MANDATORY_POS_COLUMNS:
                if c not in pos.columns:
                    column_name = c
                    error_column_name = ERROR_INFO.format(column_name, error_name)
                    if error_column_name not in pos.columns:
                        pos[error_column_name] = None
                    pos[error_column_name] = error_name
                    structure_ok &= False

            if structure_ok:  # All mandatory columns are in place

                pos = self.fixing_sorting_column(pos)
                pos = self.fixing_children_column(pos)
                pos = self.fixing_sku_column(pos, self.skus)
                pos = self.creating_subsequent_formulas_column(pos)

                pos = self.removing_extra_spaces_in_column(pos, 'KPI name Eng', )
                pos = self.removing_extra_spaces_in_column(pos, 'KPI name Rus')

                pos = self.removing_irrelevant_values(pos, 'Logical Operator')
                pos = self.removing_irrelevant_values(pos, 'score_func')
                pos = self.removing_irrelevant_values(pos, 'Manufacturer', by_column='Brand')
                pos = self.removing_irrelevant_values(pos, 'Manufacturer', by_column='Sub brand')
                pos = self.removing_irrelevant_values(pos, 'Brand', by_column='Sub brand')
                pos = self.removing_irrelevant_values(pos, 'Locations to include', by_column='Scenes to include')
                pos = self.removing_irrelevant_values(pos, 'Sub locations to include', by_column='Scenes to include')
                pos = self.removing_irrelevant_values(pos, 'Locations to include', by_column='Sub locations to include')

                pos = self.checking_empty_and_duplicate_values(pos, 'KPI ID')
                pos = self.checking_empty_and_duplicate_values(pos, 'KPI name Eng')
                pos = self.checking_empty_and_duplicate_values(pos, 'KPI name Rus', info_type=WARNING_INFO)
                pos = self.checking_level_values(pos)
                pos = self.checking_level_structure(pos)
                pos = self.checking_loose_parents(pos)

                pos = self.checking_name_length(pos, 'KPI name Eng', MIN_KPI_NAME_LENGTH)
                pos = self.checking_name_length(pos, 'KPI name Rus', MIN_KPI_NAME_LENGTH)

                pos = self.checking_restricted_symbols(pos, 'KPI name Eng', ALLOWED_SYMBOLS['KPI name Eng'])
                pos = self.checking_restricted_symbols(pos, 'KPI name Rus', ALLOWED_SYMBOLS['KPI name Rus'])

                pos = self.checking_weight_total(pos)
                pos = self.checking_weight_decimals(pos)

                pos = self.checking_column_single_values(pos, 'Category KPI Type',
                                                         info_type=ERROR_INFO, check_list=ALLOWED_CATEGORY_KPI_TYPES)
                pos = self.checking_column_single_values(pos, 'Category KPI Value',
                                                         info_type=ERROR_INFO, check_list=self.categories)
                pos = self.checking_column_single_values(pos, 'Formula',
                                                         info_type=ERROR_INFO, check_list=ALLOWED_FORMULAS)
                pos = self.checking_column_single_values(pos, 'Logical Operator',
                                                         info_type=ERROR_INFO, check_list=ALLOWED_LOGICAL_OPERATORS)
                pos = self.checking_column_single_values(pos, 'Type',
                                                         info_type=ERROR_INFO, check_list=ALLOWED_VALUES_TYPES)
                pos = self.checking_column_single_values(pos, 'depends on',
                                                         info_type=ERROR_INFO,
                                                         check_list=
                                                         pos['KPI name Eng'].tolist() + ADDITIONAL_DEPEND_ON_VALUES)

                pos = self.checking_parent_formula(pos)
                pos = self.checking_depend_on_logic(pos)
                pos = self.checking_values_column(pos)
                pos = self.checking_values_column(pos)

                # checking Products to exclude
                column_name = 'Products to exclude'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[column_name] and r.get('Values') and r.get('Type') == 'SKUs':
                            error_detected = True
                            error_values = [r[column_name]]
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.ean_codes:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Size
                column_name = 'Size'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[column_name]) if type(r[column_name]) == float \
                            and int(r[column_name]) == r[column_name] else r[column_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sizes:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Form Factor
                column_name = 'Form Factor'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.form_factors:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Form factors to exclude
                column_name = 'Form factors to exclude'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[column_name] and r.get('Form Factor'):
                            error_detected = True
                            error_values = [r[column_name]]
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.form_factors:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Brand
                column_name = 'Sub brand'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[column_name]) if type(r[column_name]) == float \
                            and int(r[column_name]) == r[column_name] else r[column_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_brands:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Brand
                column_name = 'Brand'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[column_name]) if type(r[column_name]) == float \
                            and int(r[column_name]) == r[column_name] else r[column_name]
                        for value in unicode(values).split(', '):
                            if value not in self.brands:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Sub category
                column_name = 'Sub category'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.sub_categories:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Product Category
                column_name = 'Product Category'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.categories:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Manufacturer
                column_name = 'Manufacturer'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.manufacturers:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking shelf_number
                column_name = 'shelf_number'
                error_name = 'INCORRECT VALUES'
                error_column_name = ERROR_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if not value.isdigit():
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Zone to include
                column_name = 'Zone to include'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.store_zones:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Scenes to include
                column_name = 'Scenes to include'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split('; '):
                            if value not in self.scene_types:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = '; '.join(error_values)
                            contents_ok &= False

                # checking Scenes to exclude
                column_name = 'Scenes to exclude'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[column_name] and r.get('Scenes to include'):
                            error_detected = True
                            error_values = [r[column_name]]
                        for value in unicode(r[column_name]).split('; '):
                            if value not in self.scene_types:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = '; '.join(error_values)
                            contents_ok &= False

                # checking Sub locations to include
                column_name = 'Sub locations to include'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[column_name]) if type(r[column_name]) == float \
                            and int(r[column_name]) == r[column_name] else r[column_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Sub locations to exclude
                column_name = 'Sub locations to exclude'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[column_name]) if type(r[column_name]) == float \
                            and int(r[column_name]) == r[column_name] else r[column_name]
                        if values and r.get('Sub locations to include'):
                            error_detected = True
                            error_values = [values]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Locations to include
                column_name = 'Locations to include'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

                # checking Locations to exclude
                column_name = 'Locations to exclude'
                error_name = 'INCORRECT VALUES'
                error_column_name = WARNING_INFO.format(column_name, error_name)
                if column_name in pos.columns:
                    for i, r in pos[pos[column_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[column_name] and r.get('Locations to include'):
                            error_detected = True
                            error_values = [r[column_name]]
                        for value in unicode(r[column_name]).split(', '):
                            if value not in self.locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_column_name not in pos.columns:
                                pos[error_column_name] = None
                            pos.loc[i, error_column_name] = ', '.join(error_values)
                            contents_ok &= False

            if None and structure_ok:  # The structure is consistent

                # calculating weights for all levels
                pos['00_weight'] = None
                pos.loc[(pos['level'] == 4) & (pos['KPI Weight'].notnull()), '00_weight'] = \
                    pos[(pos['level'] == 4) & (pos['KPI Weight'].notnull())]['KPI Weight']
                pos.loc[(pos['level'] == 3) & (pos['KPI Weight'].notnull()), '00_weight'] = \
                    pos[(pos['level'] == 3) & (pos['KPI Weight'].notnull())]['KPI Weight']
                pos.loc[(pos['level'] == 2) & (pos['KPI Weight'].notnull()), '00_weight'] = \
                    pos[(pos['level'] == 2) & (pos['KPI Weight'].notnull())]['KPI Weight']
                for i, r in pos[(pos['level'] == 3) & (pos['KPI Weight'].isnull()) & (pos['Children'].notnull())]\
                        .iterrows():
                    if len(pos[(pos['Parent'] == r['KPI ID']) & (pos['KPI Weight'].notnull())]) > 0:
                        pos.loc[i, '00_weight'] = pos[pos['Parent'] == r['KPI ID']]['KPI Weight'].sum()
                for i, r in pos[(pos['level'] == 2) & (pos['KPI Weight'].isnull()) & (pos['Children'].notnull())]\
                        .iterrows():
                    if len(pos[(pos['Parent'] == r['KPI ID']) & pos['KPI Weight'].notnull()]) > 0:
                        pos.loc[i, '00_weight'] = pos[pos['Parent'] == r['KPI ID']]['KPI Weight'].sum()

                # building old kpi structure columns
                pos['00_kpi_set_name'] = None
                pos['00_kpi_name'] = None
                pos['00_kpi_weight'] = None
                pos['00_kpi_atomic_name'] = None
                pos['00_kpi_atomic_display_name'] = None
                pos['00_kpi_atomic_display_name_rus'] = None
                pos['00_kpi_atomic_wight'] = None
                for i, r in pos[(pos['level'] == 2) & (pos['Children'].isnull())].iterrows():
                    pos.loc[i, '00_kpi_set_name'] = pos_name
                    pos.loc[i, '00_kpi_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_weight'] = r['00_weight']
                    pos.loc[i, '00_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '00_kpi_atomic_wight'] = r['00_weight']
                for i, r in pos[pos['level'] == 3].iterrows():
                    pos.loc[i, '00_kpi_set_name'] = pos_name
                    pos.loc[i, '00_kpi_name'] = pos[pos['KPI ID'] == r['Parent']]['KPI name Eng'].values[0]
                    pos.loc[i, '00_kpi_weight'] = pos[pos['KPI ID'] == r['Parent']]['00_weight'].values[0]
                    pos.loc[i, '00_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '00_kpi_atomic_wight'] = r['00_weight']
                for i, r in pos[pos['level'] == 4].iterrows():
                    pos.loc[i, '00_kpi_set_name'] = pos_name
                    pos.loc[i, '00_kpi_name'] = pos[pos['KPI ID'] == r['Parent']]['00_kpi_name'].values[0]
                    pos.loc[i, '00_kpi_weight'] = pos[pos['KPI ID'] == r['Parent']]['00_kpi_weight'].values[0]
                    pos.loc[i, '00_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '00_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '00_kpi_atomic_wight'] = r['00_weight']

                # checking POS name in the DB static.kpi_set
                column_name = 'PoS name'
                error_name = 'NOT IN DB (kpi_set)'
                error_column_name = ERROR_INFO.format(column_name, error_name)
                check_list = self.kpi_names['kpi_set_name'].unique().tolist()
                if pos_name not in check_list:
                    pos[error_column_name] = pos[column_name]
                    contents_ok &= False

                # checking KPI name Eng names in the DB static.kpi
                column_name = 'KPI name Eng'
                error_name = 'NOT IN DB (kpi)'
                error_column_name = ERROR_INFO.format(column_name, error_name)
                check_list = self.kpi_names[self.kpi_names['kpi_set_name'] == pos_name]['kpi_name'].unique().tolist()
                for i, r in pos[pos['level'] == 2].iterrows():
                    if r[column_name] not in check_list:
                        if error_column_name not in pos.columns:
                            pos[error_column_name] = None
                        pos.loc[i, error_column_name] = unicode(r[column_name])
                        contents_ok &= False

                # checking KPI name Eng names in the DB static.atomic_kpi
                column_name = 'KPI name Eng'
                error_name = 'NOT IN DB (atomic_kpi)'
                error_column_name = ERROR_INFO.format(column_name, error_name)
                for i, r in pos[pos['level'] in [3, 4]].iterrows():
                    check_list = self.kpi_names[(self.kpi_names['kpi_set_name'] == r['00_kpi_set_name']) &
                                                (self.kpi_names['kpi_name'] == r['00_kpi_name'])][
                        'atomic_kpi_name'].unique().tolist()
                    if r[column_name] not in check_list:
                        if error_column_name not in pos.columns:
                            pos[error_column_name] = None
                        pos.loc[i, error_column_name] = unicode(r[column_name])
                        contents_ok &= False

                # checking KPI name Eng names in the DB static.kpi_level_2
                column_name = 'KPI name Eng'
                error_name = 'NOT IN DB (kpi_level_2)'
                error_column_name = ERROR_INFO.format(column_name, error_name)
                check_list = self.kpi_level_2_names
                for i, r in pos.iterrows():
                    kpi_level_2_name = unicode(r[column_name]).upper()
                    if kpi_level_2_name not in check_list:
                        if error_column_name not in pos.columns:
                            pos[error_column_name] = None
                        pos.loc[i, error_column_name] = kpi_level_2_name
                        contents_ok &= False

            pos_all = pos_all.append(pos, ignore_index=True)

        if not any([c.find('ERROR') >= 0 for c in pos_all.columns]):

            # creating kpis file for the DB old structure
            kpi_set_name_column = ERROR_INFO.format('PoS name', 'NOT IN DB (kpi_set)')
            kpi_name_column = ERROR_INFO.format('KPI name Eng', 'NOT IN DB (kpi)')
            atomic_kpi_name_column = ERROR_INFO.format('KPI name Eng', 'NOT IN DB (atomic_kpi)')
            pos_all['NEW'] = None
            pos_all.loc[pos_all[kpi_set_name_column].notnull() |
                        pos_all[kpi_name_column].notnull() |
                        pos_all[atomic_kpi_name_column].notnull(), 'NEW'] = 'NEW'
            db_kpis = pos_all[pos_all['00_kpi_atomic_name'].notnull()][[
                                'NEW',
                                '00_kpi_set_name',
                                '00_kpi_name',
                                '00_kpi_weight',
                                '00_kpi_atomic_name',
                                '00_kpi_atomic_display_name',
                                '00_kpi_atomic_display_name_rus',
                                '00_kpi_atomic_wight'
                                ]].rename(columns={
                                            '00_kpi_set_name': 'KPI Level 1 Name',
                                            '00_kpi_name': 'KPI Level 2 Name',
                                            '00_kpi_weight': 'KPI Level 2 Weight',
                                            '00_kpi_atomic_name': 'KPI Level 3 Name',
                                            '00_kpi_atomic_display_name': 'KPI Level 3 Display Text',
                                            '00_kpi_atomic_display_name_rus': 'KPI Level 3 Display Text RUS',
                                            '00_kpi_atomic_wight': 'KPI Level 3 Weight'
                                        })
            writer = pd.ExcelWriter(os.path.join(POS_PATH_VALIDATION_OUTPUT, POS_KPIS_INTO_DB_OLD_FILE),
                                    engine='xlsxwriter')
            db_kpis.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()
            pos_all = pos_all.drop(columns=['NEW'])

            # creating kpis file for the DB new structure
            kpi_name_column = ERROR_INFO.format('KPI name Eng', 'NOT IN DB (kpi_level_2)')
            bd_kpis = pos_all[pos_all[kpi_name_column].notnull()][[kpi_name_column, 'KPI name RUS']]
            bd_kpis[KPI_LEVEL_2_INSERT] = \
                bd_kpis.apply(lambda r: KPI_LEVEL_2_VALUES.format(r[kpi_name_column] + r['KPI name RUS']), axis=1)
            writer = pd.ExcelWriter(os.path.join(POS_PATH_VALIDATION_OUTPUT, POS_KPIS_INTO_DB_NEW_FILE),
                                    engine='xlsxwriter')
            db_kpis.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()

        for c in pos_all.columns:
            if c.find('00_kpi') == 0 or c in ['00_weight']:
                pos_all = pos_all.drop(columns=[c])

        for c in pos_all.columns:
            if not (c.find('00_') == 0 or c.find('ERROR') == 0 or c.find('WARNING') == 0 or c in POS_COLUMNS):
                pos_all = pos_all.rename(columns={c: 'WARNING - ' + c + ' - IRRELEVANT COLUMN REMOVED'})

        pos_all = pos_all.where((pd.notnull(pos_all)), None)
        pos_cols = sorted(list(set(pos_all.columns) - set(POS_COLUMNS))) + POS_COLUMNS
        pos_all = pos_all.reindex(columns=pos_cols)

        # # creating all POS combined in one file
        # writer = pd.ExcelWriter(os.path.join(POS_OUTPUT_PATH, POS_ALL_FILE), engine='xlsxwriter')
        # pos_all.to_excel(writer, sheet_name='Sheet1', index=False)
        # writer.save()

        # creating POS files
        pos_files = pos_list_df['File_out'].unique().tolist()
        for pos_file in pos_files:
            writer = pd.ExcelWriter(os.path.join(POS_PATH_VALIDATION_OUTPUT, pos_file), engine='xlsxwriter')

            pos_sheets = pos_list_df[pos_list_df['File_out'] == pos_file]['Sheet_out'].unique().tolist()
            for pos_sheet in pos_sheets:

                pos_names = pos_list_df[(pos_list_df['File_out'] == pos_file) &
                                        (pos_list_df['Sheet_out'] == pos_sheet)]['POS'].unique().tolist()
                pos = pos_all[pos_all['PoS name'].isin(pos_names)]

                pos_cols = []  # keeping only relevant columns
                for c in pos.columns:
                    if c.find('ERROR') == 0 or c.find('WARNING') == 0:
                        if any(pos[c]):
                            pos_cols += [c]
                pos_cols += POS_COLUMNS
                pos = pos[pos_cols]

                ncols = self.xl_col_to_name(len(pos.columns))
                nrows = len(pos)
                pos.to_excel(writer, sheet_name=pos_sheet, index=False)

                ws = writer.sheets[pos_sheet]
                for i in range(0, nrows + 1):
                    ws.set_row(i, 20)
                ws.set_column('A:{}'.format(ncols), 30)
                ws.freeze_panes('A2')
                ws.autofilter('A1:{}{}'.format(ncols, nrows))
                ws.set_zoom(75)

            writer.save()

    @staticmethod
    def fixing_sorting_column(pos):
        pos['Sorting'] = range(1, len(pos.index) + 1)
        return pos
    
    @staticmethod
    def fixing_children_column(pos):
        for i, r in pos.iterrows():
            children = pos[pos['Parent'] == r['KPI ID']]['KPI ID'].tolist()
            children = list(set(children) - {None})
            if children:
                children = [(unicode(int(x))) for x in children]
                children = '\n'.join(children)
            else:
                children = None
            pos.loc[i, 'Children'] = children
        return pos

    @staticmethod
    def fixing_sku_column(pos, skus):
        pos['SKU'] = None
        for i, r in pos[pos['Type'] == 'SKUs'].iterrows():
            ean_list = unicode(r['Values']).split(', ')
            sku_list = []
            for ean in ean_list:
                sku_list += skus[skus['ean_code'] == ean]['name'].unique().tolist()
            pos.loc[i, 'SKU'] = ', '.join(skus)
        return pos

    @staticmethod
    def creating_subsequent_formulas_column(pos):
        for i, r in pos[pos['Children'].notnull() & pos['level'].isin([2, 3])].iterrows():
            pos.loc[i, '00_subsequent_formulas'] = \
                r['Formula'] + ': ' + ' | '.join(pos[pos['Parent'] == r['KPI ID']]['Formula'].unique().tolist())
        return pos
    
    @staticmethod
    def removing_extra_spaces_in_column(pos, column_name):
        if column_name in pos.columns:

            info_name = 'EXTRA SPACES REMOVED'
            info_type = WARNING_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            column_name_original = column_name + ' ORIGINAL'
            pos[column_name_original] = pos[column_name]
            pos[column_name] = \
                pos[column_name] \
                .astype(unicode) \
                .str.replace('\n', ' ') \
                .str.replace('  ', ' ') \
                .str.replace('  ', ' ') \
                .str.strip()

            pos_filter = pos[column_name] != pos[column_name_original]
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name_original].astype(unicode)
            pos = pos.drop(columns=[column_name_original])

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def removing_irrelevant_values(pos, column_name, by_column=None):
        if column_name in pos.columns:

            info_name = 'EXTRA SPACES REMOVED'
            info_type = WARNING_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            if column_name == 'Logical operator':
                pos_filter = pos[column_name].notnull() & \
                             ~((pos['KPI name Eng'] in ALLOWED_LOGICAL_OPERATOR_KPIS) & pos['Children'].notnull())
            elif column_name == 'score_func':
                pos_filter = pos[column_name].notnull() & pos['Target'].isnull()
            elif column_name in ['score_func', 'Manufacturer', 'Brand', 'Locations to include']:
                pos_filter = pos[column_name].notnull() & pos[by_column].notnull()
            else:
                pos[info_column_name] = 'CANNOT BE PROCESSED'
                return pos

            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)
            pos.loc[pos_filter, column_name] = None

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_empty_and_duplicate_values(pos, column_name, info_type=ERROR_INFO):
        if column_name in pos.columns:

            info_name = 'EMPTY OR DUPLICATE'
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            duplicates = pos[pos[column_name].notnull()][[column_name, 'Sorting']].groupby(column_name).count() \
                .reset_index().rename(columns={'Sorting': 'count'})
            duplicates_list = duplicates[duplicates['count'] > 1][column_name].unique().tolist()
            if duplicates_list:
                pos.loc[pos[column_name].isin(duplicates_list), info_column_name] = \
                    pos[pos[column_name].isin(duplicates_list)][column_name].astype(unicode)

            pos_filter = pos[column_name].isnull()
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_level_values(pos):
        column_name = 'level'
        if column_name in pos.columns:

            info_name = 'INCORRECT VALUE'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            pos_filter = pos['level'].isnull() | ~pos['level'].isin([1, 2, 3, 4])
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])
        
        return pos

    @staticmethod
    def checking_level_structure(pos):
        column_name = 'level'
        if column_name in pos.columns:

            info_name = 'INCORRECT STRUCTURE'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            for i, r0 in pos[pos['Children'].notnull()].iterrows():
                if r0[column_name] == 4:
                    pos.loc[i, info_column_name] = unicode(r0[column_name])
                if r0[column_name] not in ALLOWED_FORMULAS_LEVELS.get(r0['Formula'], []):
                    pos.loc[i, info_column_name] = unicode(r0[column_name])
                level = None
                for j, r1 in pos[pos['Parent'] == r0['KPI ID']].iterrows():
                    level = r1[column_name] if level is None else level
                    if r1[column_name] != level \
                            or not (level in [1, 2] and r0[column_name] == 1 or
                                    level in [3] and r0[column_name] == 2 or
                                    level in [4] and r0[column_name] == 3):
                        pos.loc[j, info_column_name] = unicode(r1[column_name])

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])
        
        return pos

    @staticmethod
    def checking_loose_parents(pos):
        column_name = 'Parent'
        if column_name in pos.columns:

            info_name = 'LOOSE'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            kpi_ids = pos['KPI ID'].tolist()
            pos_filter = ((pos['level'] == 1) & pos[column_name].notnull() | (pos['level'] != 1)) \
                         & ~pos[column_name].isin(kpi_ids)
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_name_length(pos, column_name, length):
        if column_name in pos.columns:

            info_name = 'TOO SHORT (<{})'.format(length)
            info_type = WARNING_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            pos_filter = pos[column_name].astype(unicode).str.len() < length
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_restricted_symbols(pos, column_name, allowed_symbols):
        if column_name in pos.columns:

            info_name = 'RESTRICTED SYMBOLS'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            for i, r in pos.iterrows():
                if any((s not in allowed_symbols) for s in unicode(r[column_name])):
                    pos.loc[i, info_column_name] = unicode(r[column_name])

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_weight_total(pos):
        column_name = 'KPI Weight'
        if column_name in pos.columns:
            total_weight = round(pos[column_name].sum(), 7)
            if total_weight != 1.0:
                info_name = 'TOTAL != 1.0'
                info_type = ERROR_INFO
                info_column_name = info_type.format(column_name, info_name)
                pos[info_column_name] = 'TOTAL = ' + str(total_weight)
        return pos

    @staticmethod
    def checking_weight_decimals(pos):
        column_name = 'KPI Weight'
        if column_name in pos.columns:

            info_name = 'DECIMALS > 6 DIGITS'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            weights = pos[column_name].astype(unicode).str.split('.', 1, True)
            weights[2] = weights[1].str.len()
            pos_filter = (weights[2] > 6)
            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_column_single_values(pos, column_name, check_list=None, info_type=ERROR_INFO):
        if column_name in pos.columns:

            check_list = check_list if check_list else []

            info_name = 'INCORRECT VALUE'
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            if column_name == 'Category KPI Type':
                pos_filter = pos[column_name].notnull() & \
                             (~pos[column_name].isin(check_list) | pos['level'] != 2)
            elif column_name == 'Category KPI Value':
                pos_filter = pos['Category KPI Type'].notnull() & ~pos[column_name].isin(check_list)
            elif column_name == 'Formula':
                pos_filter = ~pos[column_name].isin(check_list)
            elif column_name == 'Logical Operator':
                pos_filter = (pos['KPI name Eng'].isin(ALLOWED_LOGICAL_OPERATOR_KPIS) & pos['Children'].notnull()) & \
                             ~pos[column_name].isin(check_list)
            elif column_name == 'Type':
                pos_filter = (pos[column_name].notnull() | pos['Values'].notnull()) & \
                             ~pos[column_name].isin(check_list)
            elif column_name == 'depends on':
                pos_filter = pos[column_name].notnull() & ~pos[column_name].isin(check_list)
            else:
                pos[info_column_name] = 'CANNOT BE PROCESSED'
                return pos

            pos.loc[pos_filter, info_column_name] = pos[pos_filter][column_name].astype(unicode)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_parent_formula(pos):
        column_name = 'Formula'
        if column_name in pos.columns:

            info_name = 'INCORRECT PARENT FORMULA'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            for i, r in pos[pos['Parent'].notnull()].iterrows():
                parent_formula = pos[pos['KPI ID'] == r['Parent']][column_name].tolist()
                parent_formula = parent_formula[0] if parent_formula else None
                allowed_formulas = ALLOWED_FORMULAS_PARENTS.get(r[column_name], [])
                if parent_formula not in allowed_formulas:
                    pos.loc[i, info_column_name] = unicode(parent_formula) + ' X<= ' + unicode(r[column_name])

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    @staticmethod
    def checking_depend_on_logic(pos):
        column_name = 'depends on'
        if column_name in pos.columns:

            info_name = 'INCORRECT DEPENDENCY'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            for i, r in pos[pos[column_name].notnull()].iterrows():
                if r[column_name] in ADDITIONAL_DEPEND_ON_VALUES:
                    depends_on = r['Formula']
                    depends_on_formula = r[column_name]
                else:
                    depends_on = r['Formula']
                    depends_on_formula = pos[pos['KPI name Eng'] == r[column_name]]['Formula'].tolist()
                    depends_on_formula = depends_on_formula[0] if depends_on_formula else None
                allowed_formulas = ALLOWED_FORMULAS_DEPENDENCIES.get(depends_on, [])
                if depends_on_formula not in allowed_formulas:
                    pos.loc[i, info_column_name] = unicode(depends_on_formula) + ' X<= ' + unicode(depends_on)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    def checking_values_column(self, pos):
        column_name = 'Values'
        if column_name in pos.columns:

            info_name = 'INCORRECT VALUES'
            info_type = ERROR_INFO
            info_column_name = info_type.format(column_name, info_name)
            if info_column_name not in pos.columns:
                pos[info_column_name] = None

            for i, r in pos[pos['Type'].notnull()].iterrows():
                error_detected = False
                error_values = []
                values = int(r[column_name]) \
                    if type(r[column_name]) == float and int(r[column_name]) == r[column_name] \
                    else r[column_name]
                if r['Type'] == 'MAN' and not r.get('Manufacturer'):
                    for value in unicode(values).split(', '):
                        if value not in self.manufacturers:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] in ('CAT', 'MAN in CAT'):
                    if r.get('Product Category') and r[column_name] != r.get('Product Category'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.categories:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] == 'SUB_CATEGORY':
                    if r.get('Sub category') and r[column_name] != r.get('Sub category'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.sub_categories:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] in ('BRAND', 'BRAND_IN_CAT'):
                    if r.get('Brand') and r[column_name] != r.get('Brand'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.brands:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] in ('SUB_BRAND', 'SUB_BRAND_IN_CAT'):
                    if r.get('Sub brand') and r[column_name] != r.get('Sub brand'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.sub_brands:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] == 'SKUs':
                    for value in unicode(values).split(', '):
                        if value not in self.ean_codes:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] == 'SCENE':
                    if r.get('Scenes to include') and r[column_name] != r.get('Scenes to include'):
                        error_detected = True
                    for value in unicode(values).split('; '):
                        if value not in self.scene_types:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] == 'SUB_LOCATION_TYPE':
                    if r.get('Sub locations to include') and r[column_name] != r.get('Sub locations to include'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.sub_locations:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] == 'LOCATION_TYPE':
                    if r.get('Locations to include') and r[column_name] != r.get('Locations to include'):
                        error_detected = True
                    for value in unicode(values).split(', '):
                        if value not in self.locations:
                            error_detected = True
                            error_values += [value]
                elif r['Type'] in ('NUM_SCENES', 'DOORS'):
                    if values:
                        error_detected = True
                        error_values += [values]
                if error_detected:
                    pos.loc[i, info_column_name] = r['Type'] + ': ' + ', '.join(error_values)

            if not any(pos[info_column_name]):
                pos.drop(columns=[info_column_name])

        return pos

    def checking_list_values(self, pos):


        # checking Products to exclude
        column_name = 'Products to exclude'
        error_name = 'INCORRECT VALUES'
        error_column_name = WARNING_INFO.format(column_name, error_name)
        if column_name in pos.columns:
            for i, r in pos[pos[column_name].notnull()].iterrows():
                error_detected = False
                error_values = []
                if r[column_name] and r.get('Values') and r.get('Type') == 'SKUs':
                    error_detected = True
                    error_values = [r[column_name]]
                for value in unicode(r[column_name]).split(', '):
                    if value not in self.ean_codes:
                        error_detected = True
                        error_values += [value]
                if error_detected:
                    if error_column_name not in pos.columns:
                        pos[error_column_name] = None
                    pos.loc[i, error_column_name] = ', '.join(error_values)
                    contents_ok &= False

    def get_kpi_names(self):
        query = """
                SELECT 
                    s.pk as kpi_set_pk, 
                    s.name as kpi_set_name, 
                    k.pk as kpi_pk,
                    k.display_text as kpi_name,
                    a.pk as atomic_kpi_pk,
                    a.name as atomic_kpi_name
                FROM static.kpi_set s
                JOIN static.kpi k ON k.kpi_set_fk=s.pk
                LEFT JOIN static.atomic_kpi a ON a.kpi_fk=k.pk;
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_kpi_level_2_names(self):
        query = """
                SELECT type as name
                FROM static.kpi_level_2
                WHERE initiated_by='Entity';
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_manufacturers(self):
        query = """
                SELECT name 
                FROM static_new.manufacturer 
                WHERE delete_date IS NULL;
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_categories(self):
        query = """
                SELECT name 
                FROM static_new.category 
                WHERE delete_date IS NULL;
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_sub_categories(self):
        query = """
                SELECT name 
                FROM static_new.sub_category 
                WHERE delete_date IS NULL;
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_brands(self):
        query = """
                SELECT name 
                FROM static_new.brand 
                WHERE delete_date IS NULL;
                """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_sub_brands(self):
        query = \
            """
            SELECT DISTINCT REPLACE(JSON_EXTRACT(product.labels, '$.sub_brand'), '"', '') AS name
            FROM static_new.product
            WHERE delete_date IS NULL
            HAVING name IS NOT NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_skus(self):
        query = \
            """
            SELECT ean_code, name
            FROM static_new.product
            WHERE delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_sizes(self):
        query = \
            """
            SELECT DISTINCT CAST(size AS CHAR(10)) AS size
            FROM static_new.product
            WHERE size IS NOT NULL AND delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_form_factors(self):
        query = \
            """
            SELECT DISTINCT form_factor AS name
            FROM static_new.product
            WHERE form_factor IS NOT NULL AND delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_scene_types(self):
        query = \
            """
            SELECT name
            FROM static.template
            WHERE delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_sub_locations(self):
        query = \
            """
            SELECT DISTINCT additional_attribute_2 as name
            FROM static.template
            WHERE additional_attribute_2 IS NOT NULL AND delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_locations(self):
        query = \
            """
            SELECT name
            FROM static.location_types
            WHERE delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_store_zones(self):
        query = \
            """
            SELECT name
            FROM static.store_task_area_group_items;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    @staticmethod
    def xl_col_to_name(num):
        letters = ''
        while num:
            mod = (num - 1) % 26
            letters += chr(mod + 65)
            num = (num - 1) // 26
        return ''.join(reversed(letters))


if __name__ == '__main__':
    kpis_list = CCRU_SANDPOSValidator()
    kpis_list.validate_and_transform()
