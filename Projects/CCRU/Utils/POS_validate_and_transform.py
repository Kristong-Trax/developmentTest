# - *- coding: utf- 8 - *-
import os
import sys
import json
import pandas as pd
import xlsxwriter as xl

from Trax.Data.Orm.OrmCore import OrmSession


__author__ = 'sergey'


sys.path.append('.')

PROJECT = 'ccru'

POS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data/KPIs_2020')
POS_PATH_INPUT = os.path.join(POS_PATH, 'INPUT')
POS_OUTPUT_PATH = os.path.join(POS_PATH, 'OUTPUT')
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

ERROR_FIELD_NAME = 'ERROR - {} - {}'
WARNING_FIELD_NAME = 'WARNING - {} - {}'

POS_COLUMNS = [
    'Sorting',
    'PoS name',

    'level',
    'KPI ID',
    'Children',
    'Parent',

    'KPI Weight',

    'KPI name Eng',
    'KPI name Rus',

    'Category KPI Type',
    'Category KPI Value',

    'Formula',
    'Logical Operator',

    # 'Result Format',
    'Target',
    'target_min',
    'target_max',
    'score_func',
    'score_min',
    'score_max',

    'depends on',

    'Type',
    'Values',
    'SKU',

    'Products to exclude',
    'Size',
    'Form Factor',
    'Sub brand',
    'Brand',
    'Manufacturer',
    'Sub category',
    'Product Category',

    'shelf_number',
    'Zone to include',
    'Scenes to include',
    'Scenes to exclude',
    'Sub locations to include',
    'Sub locations to exclude',
    'Locations to exclude',
    'Locations to include',

    'Comments',

    'SAP PoS',
    'Channel',
    'KPI Type',
    'SAP KPI',
]

MANDATORY_POS_COLUMNS = [
    'level',
    'KPI ID',
    'Parent',
    'KPI Weight',
    'KPI name Eng',
    'KPI name Rus',
    'Formula',
    'Target',
    'score_func',
    'Type',
    'Values'
]

ALLOWED_SYMBOLS_KPI_NAME_ENG = ' ' \
                            '0123456789' \
                            'ABCDEFGHIJKLMNOPQRSTUVWXWZ' \
                            'abcdefghijklmnopqrstuvwxyz' \
                            '%&()[]-_+/<>.:'
ALLOWED_SYMBOLS_KPI_NAME_RUS = u' ' \
                               u'0123456789' \
                               u'ABCDEFGHIJKLMNOPQRSTUVWXWZ' \
                               u'abcdefghigklmnopqrstuvwxyz' \
                               u'%&()[]-_+/<>.:' \
                               u'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ' \
                               u'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
ALLOWED_FORMULAS_PARENTS = \
    {
        'check_number_of_scenes_with_facings_target':
            ['sum of atomic KPI result',
             'number of atomic KPI Passed',
             'Weighted Average'],
        'each SKU hits facings target':
            ['Weighted Average'],
        'facings TCCC/40':
            ['sum of atomic KPI result',
             'Group'],
        'Group':
            ['Group'],
        'Lead SKU':
            ['number of atomic KPI Passed',
             'number of atomic KPI Passed on the same scene',
             'number of sub atomic KPI Passed'],
        'number of atomic KPI Passed':
            ['Group'],
        'number of atomic KPI Passed on the same scene':
            ['Group'],
        'number of coolers with facings target and fullness target':
            ['sum of atomic KPI result'],
        'number of doors of filled Coolers':
            ['sum of atomic KPI result',
             'number of atomic KPI Passed'],
        'number of doors with more than Target facings':
            ['sum of atomic KPI result',
             'number of atomic KPI Passed',
             'sum of atomic KPI result'],
        'number of facings':
            ['number of facings',
             'Weighted Average',
             'number of atomic KPI Passed',
             'number of atomic KPI Passed on the same scene',
             'number of sub atomic KPI Passed',
             'Group'],
        'number of pure Coolers':
            ['number of atomic KPI Passed',
             'Group'],
        'number of scenes':
            ['number of atomic KPI Passed',
             'number of atomic KPI Passed on the same scene',
             'Group'],
        'number of SKU per Door RANGE':
            ['Group'],
        'number of SKU per Door RANGE TOTAL':
            ['Group'],
        'number of sub atomic KPI Passed':
            ['number of atomic KPI Passed',
             'number of atomic KPI Passed on the same scene',
             'Weighted Average'],
        'Scenes with no tagging':
            ['number of atomic KPI Passed',
             'Weighted Average',
             'Group'],
        # 'Share of CCH doors which do not have FC packs':
        #     [],
        'Share of CCH doors which have 98% TCCC facings':
            ['Group'],
        'SOS':
            ['Weighted Average',
             'Group'],
        'sum of atomic KPI result':
            ['Group'],
        'Weighted Average':
            ['Group'],
    }
ALLOWED_FORMULAS = ALLOWED_FORMULAS_PARENTS.keys()

ALLOWED_FORMULAS_DEPENDENCIES = \
    {
        'filled collers target':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'scene type':
            ['number of sub atomic KPI Passed'],
        'check_number_of_scenes_with_facings_target':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'each SKU hits facings target':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'number of facings':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'number of pure Coolers':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'number of SKU per Door RANGE':
            ['number of doors of filled Coolers',
             'number of coolers with facings target and fullness target',
             'filled collers target'],
        'number of SKU per Door RANGE TOTAL':
            ['number of doors of filled Coolers',
             'number of coolers with facings target and fullness target'],
        'number of sub atomic KPI Passed':
            [],
        # 'Share of CCH doors which do not have FC packs':
        #     [],
        'Share of CCH doors which have 98% TCCC facings':
            ['number of doors of filled Coolers',
             'number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
        'Weighted Average':
            ['number of doors with more than Target facings',
             'number of coolers with facings target and fullness target'],
    }

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


class CCRUKPIS:

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

    @staticmethod
    def xl_col_to_name(num):
        letters = ''
        while num:
            mod = (num - 1) % 26
            letters += chr(mod + 65)
            num = (num - 1) // 26
        return ''.join(reversed(letters))

    def validate_and_transform(self):

        pos_list_df = pd.read_excel(os.path.join(POS_PATH, POS_LIST['file_name']),
                                    sheet_name=POS_LIST['sheet_name'], convert_float=True)
        pos_list_df = pos_list_df.where((pd.notnull(pos_list_df)), None)

        pos_all = pd.DataFrame(columns=POS_COLUMNS)
        for index, row in pos_list_df.iterrows():
            pos_name = row['POS']
            file_in = row['File_in']
            sheet_in = row['Sheet_in'] if row['Sheet_in'] else 0
            file_out = row['File_out']
            sheet_out = row['Sheet_out'] if row['Sheet_out'] else 'Sheet1'

            pos = pd.read_excel(os.path.join(POS_PATH_INPUT, file_in),
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

            structure_is_ok = True
            contents_is_ok = True

            # stripping all data from leading and trailing extra spaces
            columns = pos.columns
            for i, r in pos.iterrows():
                for c in columns:
                    if type(r[c]) in (str, unicode):
                        pos.loc[i, c] = r[c].strip()

            # checking mandatory columns
            error_name = 'COLUMN IS NOT FOUND'
            for c in MANDATORY_POS_COLUMNS:
                if c not in pos.columns:
                    field_name = c
                    error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                    if error_field_name not in pos.columns:
                        pos[error_field_name] = None
                    pos[error_field_name] = error_name
                    structure_is_ok = False

            if structure_is_ok:  # All mandatory columns are in place

                # fixing Sorting
                pos['Sorting'] = range(1, len(pos.index) + 1)

                # fixing Children
                for i, r in pos.iterrows():
                    children = pos[pos['Parent'] == r['KPI ID']]['KPI ID'].tolist()
                    children = list(set(children) - {None})
                    if children:
                        children = [(unicode(int(x))) for x in children]
                        children = '\n'.join(children)
                    else:
                        children = None
                    pos.loc[i, 'Children'] = children

                # fixing SKUs
                pos['SKU'] = None
                for i, r in pos[pos['Type'] == 'SKUs'].iterrows():
                    eans = unicode(r['Values']).split(', ')
                    skus = []
                    for ean in eans:
                        skus += self.skus[self.skus['ean_code'] == ean]['name'].unique().tolist()
                    pos.loc[i, 'SKU'] = ', '.join(skus)

                # subsequent formulas
                for i, r in pos[pos['Children'].notnull() & pos['level'].isin([2, 3])].iterrows():
                    pos.loc[i, '00_subsequent_formulas'] = \
                        r['Formula'] + ': ' + ' | '.join(pos[pos['Parent'] == r['KPI ID']]['Formula'].unique().tolist())

                # removing internal extra spaces in KPI names Eng
                field_name = 'KPI name Eng'
                error_name = 'EXTRA SPACES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        kpi_name_stripped = unicode(r[field_name])\
                            .replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip() \
                            if r[field_name] is not None else None
                        if r[field_name] != kpi_name_stripped:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = kpi_name_stripped

                # removing internal extra spaces in KPI names Rus
                field_name = 'KPI name Rus'
                error_name = 'EXTRA SPACES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        kpi_name_stripped = unicode(r[field_name])\
                            .replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip() \
                            if r[field_name] is not None else None
                        if r[field_name] != kpi_name_stripped:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = kpi_name_stripped

                # removing irrelevant Logical Operator
                field_name = 'Logical Operator'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if not (r['KPI name Eng'] in ALLOWED_LOGICAL_OPERATOR_KPIS and r['Children']):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant score_func
                field_name = 'score_func'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if not r['Target']:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Manufacturer by Brand
                field_name = 'Manufacturer'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Brand'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Manufacturer by Sub brand
                field_name = 'Manufacturer'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Sub brand'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Brand by Sub brand
                field_name = 'Brand'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Sub brand'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Locations to include by Scenes to include
                field_name = 'Locations to include'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Scenes to include'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Sub locations to include by Scenes to include
                field_name = 'Sub locations to include'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Scenes to include'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # removing irrelevant Locations to include by Sub locations to include
                field_name = 'Locations to include'
                error_name = 'IRRELEVANT VALUES REMOVED'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r.get('Sub locations to include'):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            pos.loc[i, field_name] = None

                # checking KPI ID
                field_name = 'KPI ID'
                error_name = 'EMPTY OR DUPLICATE'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    duplicates = pos[[field_name, 'Sorting']].groupby(field_name).count()\
                        .reset_index().rename(columns={'Sorting': 'count'})
                    duplicates_list = duplicates[duplicates['count'] > 1][field_name].unique().tolist()
                    if duplicates_list:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[pos[field_name].isin(duplicates_list), error_field_name] = \
                            pos[pos[field_name].isin(duplicates_list)][field_name].astype(unicode)
                        structure_is_ok = False

                # checking level
                pos['00_level_checked'] = None
                field_name = 'level'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    # run 1
                    incorrect_levels_mask = (pos['level'].isnull()) | ~(pos['level'].isin([1, 2, 3, 4]))
                    incorrect_levels = pos[incorrect_levels_mask]['level'].astype(unicode)
                    if len(incorrect_levels):
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[incorrect_levels_mask, error_field_name] = incorrect_levels
                        structure_is_ok = False
                    # run 2
                    for i, r0 in pos[pos['Children'].notnull()].iterrows():
                        if r0['level'] == 1 and r0['Parent'] is None:
                            pos.loc[i, '00_level_checked'] = 1
                        if r0['level'] == 4:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r0[field_name])
                            structure_is_ok = False
                        level = None
                        for j, r1 in pos[pos['Parent'] == r0['KPI ID']].iterrows():
                            pos.loc[j, '00_level_checked'] = 1
                            level = r1['level'] if level is None else level
                            if r1['level'] != level \
                                    or not (level in [1, 2] and r0['level'] == 1 or
                                            level in [3] and r0['level'] == 2 or
                                            level in [4] and r0['level'] == 3):
                                if error_field_name not in pos.columns:
                                    pos[error_field_name] = None
                                pos.loc[j, error_field_name] = unicode(r1[field_name])
                                structure_is_ok = False

                # checking loose Parent
                field_name = 'Parent'
                error_name = 'LOOSE'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    if len(pos[pos['00_level_checked'].isnull()]) > 0:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[pos['00_level_checked'].isnull(), error_field_name] = \
                            pos[pos['00_level_checked'].isnull()][field_name].astype(unicode)
                        structure_is_ok = False

                # checking length of KPI name Eng
                field_name = 'KPI name Eng'
                error_name = 'TOO SHORT (<{})'.format(MIN_KPI_NAME_LENGTH)
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        if len(unicode(r[field_name])) < MIN_KPI_NAME_LENGTH:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking length of KPI nam Rus
                field_name = 'KPI name Rus'
                error_name = 'TOO SHORT (<{})'.format(MIN_KPI_NAME_LENGTH)
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        if len(unicode(r[field_name])) < MIN_KPI_NAME_LENGTH:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking for restricted symbols in KPI names Eng
                field_name = 'KPI name Eng'
                error_name = 'RESTRICTED SYMBOLS'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        if any((s not in ALLOWED_SYMBOLS_KPI_NAME_ENG) for s in unicode(r[field_name])):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking for restricted symbols in KPI names Rus
                field_name = 'KPI name Rus'
                error_name = 'RESTRICTED SYMBOLS'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        if any((s not in ALLOWED_SYMBOLS_KPI_NAME_RUS) for s in unicode(r[field_name])):
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking for duplicate KPI name Eng
                field_name = 'KPI name Eng'
                error_name = 'DUPLICATE'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    duplicates = pos[[field_name, 'Sorting']].groupby(field_name).count()\
                        .reset_index().rename(columns={'Sorting': 'count'})
                    duplicates_list = duplicates[duplicates['count'] > 1][field_name].unique().tolist()
                    if duplicates_list:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[pos[field_name].isin(duplicates_list), error_field_name] = \
                            pos[pos[field_name].isin(duplicates_list)][field_name].astype(unicode)
                        contents_is_ok = False

                # checking total KPI Weight == 1.0
                field_name = 'KPI Weight'
                error_name = 'TOTAL != 1.0'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    total_weight = round(pos[field_name].sum(), 7)
                    if total_weight != 1.0:
                        pos[error_field_name] = 'TOTAL = ' + str(total_weight)
                        contents_is_ok = False

                # checking KPI Weight for decimals
                field_name = 'KPI Weight'
                error_name = 'DECIMALS > 6 DIGITS'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    weights = pos[field_name].astype(unicode).str.split('.', 1, True)
                    weights[2] = weights[1].str.len()
                    incorrect_precision_mask = (weights[2] > 6)
                    if len(incorrect_precision_mask) > 0:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[incorrect_precision_mask, error_field_name] = \
                            pos[incorrect_precision_mask][field_name].astype(unicode)
                        contents_is_ok = False

                # checking Category KPI Type
                field_name = 'Category KPI Type'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r[field_name] not in ALLOWED_CATEGORY_KPI_TYPES or r['level'] != 2:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking Category KPI Value
                field_name = 'Category KPI Value'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos['Category KPI Type'].notnull()].iterrows():
                        if r[field_name] not in self.categories:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking Formula
                field_name = 'Formula'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos.iterrows():
                        if r[field_name] not in ALLOWED_FORMULAS:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking Formula Parent
                field_name = 'Formula'
                error_name = 'INCORRECT PARENT FORMULA'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos['Parent'].notnull()].iterrows():
                        parent_formula = pos[pos['KPI ID'] == r['Parent']][field_name].tolist()
                        parent_formula = parent_formula[0] if parent_formula else None
                        allowed_formulas = ALLOWED_FORMULAS_PARENTS.get(r[field_name], [])
                        if parent_formula not in allowed_formulas:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(parent_formula) + ' X<= ' + unicode(r[field_name])
                            contents_is_ok = False
                            structure_is_ok = False

                # checking Logical Operator
                field_name = 'Logical Operator'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[(pos['KPI name Eng'].isin(ALLOWED_LOGICAL_OPERATOR_KPIS) &
                                     pos['Children'].notnull())].iterrows():
                        if r[field_name] not in ALLOWED_LOGICAL_OPERATORS:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking 'depends on' name
                field_name = 'depends on'
                error_name = 'INCORRECT NAME'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r[field_name] not in pos['KPI name Eng'].tolist() +\
                                ['scene type', 'filled collers target']:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking 'depends on' logic
                field_name = 'depends on'
                error_name = 'INCORRECT DEPENDENCY'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        if r[field_name] in ['scene type', 'filled collers target']:
                            depends_on = r[field_name]
                            depends_on_formula = r['Formula']
                        else:
                            depends_on = r['Formula']
                            depends_on_formula = pos[pos['KPI name Eng'] == r[field_name]]['Formula'].tolist()
                            depends_on_formula = depends_on_formula[0] if depends_on_formula else None
                        allowed_formulas = ALLOWED_FORMULAS_DEPENDENCIES.get(depends_on, [])
                        if depends_on_formula not in allowed_formulas:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(depends_on_formula) + ' X<= ' + unicode(depends_on)
                            contents_is_ok = False

                # checking Type
                field_name = 'Type'
                error_name = 'INCORRECT'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull() | pos['Values'].notnull()].iterrows():
                        if r[field_name] not in ALLOWED_VALUES_TYPES:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = unicode(r[field_name])
                            contents_is_ok = False

                # checking Values
                field_name = 'Values'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos['Type'].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        if r['Type'] == 'MAN' and not r.get('Manufacturer'):
                            for value in unicode(values).split(', '):
                                if value not in self.manufacturers:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] in ('CAT', 'MAN in CAT'):
                            if r.get('Product Category') and r[field_name] != r.get('Product Category'):
                                error_detected = True
                            for value in unicode(values).split(', '):
                                if value not in self.categories:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] == 'SUB_CATEGORY':
                            if r.get('Sub category') and r[field_name] != r.get('Sub category'):
                                error_detected = True
                            for value in unicode(values).split(', '):
                                if value not in self.sub_categories:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] in ('BRAND', 'BRAND_IN_CAT'):
                            if r.get('Brand') and r[field_name] != r.get('Brand'):
                                error_detected = True
                            for value in unicode(values).split(', '):
                                if value not in self.brands:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] in ('SUB_BRAND', 'SUB_BRAND_IN_CAT'):
                            if r.get('Sub brand') and r[field_name] != r.get('Sub brand'):
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
                            if r.get('Scenes to include') and r[field_name] != r.get('Scenes to include'):
                                error_detected = True
                            for value in unicode(values).split('; '):
                                if value not in self.scene_types:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] == 'SUB_LOCATION_TYPE':
                            if r.get('Sub locations to include') and r[field_name] != r.get('Sub locations to include'):
                                error_detected = True
                            for value in unicode(values).split(', '):
                                if value not in self.sub_locations:
                                    error_detected = True
                                    error_values += [value]
                        elif r['Type'] == 'LOCATION_TYPE':
                            if r.get('Locations to include') and r[field_name] != r.get('Locations to include'):
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
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = r['Type'] + ': ' + ', '.join(error_values)
                            contents_is_ok = False

                # checking Products to exclude
                field_name = 'Products to exclude'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[field_name] and r.get('Values') and r.get('Type') == 'SKUs':
                            error_detected = True
                            error_values = [r[field_name]]
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.ean_codes:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Size
                field_name = 'Size'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sizes:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Form Factor
                field_name = 'Form Factor'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.form_factors:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Form factors to exclude
                field_name = 'Form factors to exclude'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[field_name] and r.get('Form Factor'):
                            error_detected = True
                            error_values = [r[field_name]]
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.form_factors:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Brand
                field_name = 'Sub brand'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_brands:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Brand
                field_name = 'Brand'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        for value in unicode(values).split(', '):
                            if value not in self.brands:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Sub category
                field_name = 'Sub category'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.sub_categories:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Product Category
                field_name = 'Product Category'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.categories:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Manufacturer
                field_name = 'Manufacturer'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.manufacturers:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking shelf_number
                field_name = 'shelf_number'
                error_name = 'INCORRECT VALUES'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if not value.isdigit():
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Zone to include
                field_name = 'Zone to include'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.store_zones:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Scenes to include
                field_name = 'Scenes to include'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split('; '):
                            if value not in self.scene_types:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = '; '.join(error_values)
                            contents_is_ok = False

                # checking Scenes to exclude
                field_name = 'Scenes to exclude'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[field_name] and r.get('Scenes to include'):
                            error_detected = True
                            error_values = [r[field_name]]
                        for value in unicode(r[field_name]).split('; '):
                            if value not in self.scene_types:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = '; '.join(error_values)
                            contents_is_ok = False

                # checking Sub locations to include
                field_name = 'Sub locations to include'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Sub locations to exclude
                field_name = 'Sub locations to exclude'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        values = int(r[field_name]) if type(r[field_name]) == float \
                            and int(r[field_name]) == r[field_name] else r[field_name]
                        if values and r.get('Sub locations to include'):
                            error_detected = True
                            error_values = [values]
                        for value in unicode(values).split(', '):
                            if value not in self.sub_locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Locations to include
                field_name = 'Locations to include'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

                # checking Locations to exclude
                field_name = 'Locations to exclude'
                error_name = 'IRRELEVANT VALUES'
                error_field_name = WARNING_FIELD_NAME.format(field_name, error_name)
                if field_name in pos.columns:
                    for i, r in pos[pos[field_name].notnull()].iterrows():
                        error_detected = False
                        error_values = []
                        if r[field_name] and r.get('Locations to include'):
                            error_detected = True
                            error_values = [r[field_name]]
                        for value in unicode(r[field_name]).split(', '):
                            if value not in self.locations:
                                error_detected = True
                                error_values += [value]
                        if error_detected:
                            if error_field_name not in pos.columns:
                                pos[error_field_name] = None
                            pos.loc[i, error_field_name] = ', '.join(error_values)
                            contents_is_ok = False

            if None and structure_is_ok:  # The structure is consistent

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
                field_name = 'PoS name'
                error_name = 'NOT IN DB (kpi_set)'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                check_list = self.kpi_names['kpi_set_name'].unique().tolist()
                if pos_name not in check_list:
                    pos[error_field_name] = pos[field_name]
                    contents_is_ok = False

                # checking KPI name Eng names in the DB static.kpi
                field_name = 'KPI name Eng'
                error_name = 'NOT IN DB (kpi)'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                check_list = self.kpi_names[self.kpi_names['kpi_set_name'] == pos_name]['kpi_name'].unique().tolist()
                for i, r in pos[pos['level'] == 2].iterrows():
                    if r[field_name] not in check_list:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[i, error_field_name] = unicode(r[field_name])
                        contents_is_ok = False

                # checking KPI name Eng names in the DB static.atomic_kpi
                field_name = 'KPI name Eng'
                error_name = 'NOT IN DB (atomic_kpi)'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                for i, r in pos[pos['level'] in [3, 4]].iterrows():
                    check_list = self.kpi_names[(self.kpi_names['kpi_set_name'] == r['00_kpi_set_name']) &
                                                (self.kpi_names['kpi_name'] == r['00_kpi_name'])][
                        'atomic_kpi_name'].unique().tolist()
                    if r[field_name] not in check_list:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[i, error_field_name] = unicode(r[field_name])
                        contents_is_ok = False

                # checking KPI name Eng names in the DB static.kpi_level_2
                field_name = 'KPI name Eng'
                error_name = 'NOT IN DB (kpi_level_2)'
                error_field_name = ERROR_FIELD_NAME.format(field_name, error_name)
                check_list = self.kpi_level_2_names
                for i, r in pos.iterrows():
                    kpi_level_2_name = unicode(r[field_name]).upper()
                    if kpi_level_2_name not in check_list:
                        if error_field_name not in pos.columns:
                            pos[error_field_name] = None
                        pos.loc[i, error_field_name] = kpi_level_2_name
                        contents_is_ok = False

            pos_all = pos_all.append(pos, ignore_index=True)

        if not any([c.find('ERROR') >= 0 for c in pos_all.columns]):

            # creating kpis file for the DB old structure
            kpi_set_name_column = ERROR_FIELD_NAME.format('PoS name', 'NOT IN DB (kpi_set)')
            kpi_name_column = ERROR_FIELD_NAME.format('KPI name Eng', 'NOT IN DB (kpi)')
            atomic_kpi_name_column = ERROR_FIELD_NAME.format('KPI name Eng', 'NOT IN DB (atomic_kpi)')
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
            writer = pd.ExcelWriter(os.path.join(POS_OUTPUT_PATH, POS_KPIS_INTO_DB_OLD_FILE), engine='xlsxwriter')
            db_kpis.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()
            pos_all = pos_all.drop(columns=['NEW'])

            # creating kpis file for the DB new structure
            kpi_name_column = ERROR_FIELD_NAME.format('KPI name Eng', 'NOT IN DB (kpi_level_2)')
            bd_kpis = pos_all[pos_all[kpi_name_column].notnull()][[kpi_name_column, 'KPI name RUS']]
            bd_kpis[KPI_LEVEL_2_INSERT] = \
                bd_kpis.apply(lambda r: KPI_LEVEL_2_VALUES.format(r[kpi_name_column] + r['KPI name RUS']), axis=1)
            writer = pd.ExcelWriter(os.path.join(POS_OUTPUT_PATH, POS_KPIS_INTO_DB_NEW_FILE), engine='xlsxwriter')
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
            writer = pd.ExcelWriter(os.path.join(POS_OUTPUT_PATH, pos_file), engine='xlsxwriter')

            pos_sheets = pos_list_df[pos_list_df['File_out'] == pos_file]['Sheet_out'].unique().tolist()
            for pos_sheet in pos_sheets:

                pos_names = pos_list_df[(pos_list_df['File_out'] == pos_file) &
                                        (pos_list_df['Sheet_out'] == pos_sheet)]['POS'].unique().tolist()
                pos = pos_all[pos_all['PoS name'].isin(pos_names)]

                pos_cols = []  # keeping only relevant columns
                for c in pos.columns:
                    if c.find('ERROR') == 0 or c.find('WARNING') == 0 or True:
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

    def get_kpi_names(self):
        query = """
                SELECT 
                    s.name as kpi_set_name, 
                    k.display_text as kpi_name,
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
            SELECT REPLACE(JSON_EXTRACT(product.labels, '$.sub_brand'), '"', '') AS name
            FROM static_new.product
            WHERE delete_date IS NULL;
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
            SELECT CAST(size AS CHAR(10)) AS size
            FROM static_new.product
            WHERE size IS NOT NULL AND delete_date IS NULL;
            """
        query_results = self.rds_conn.execute(query.replace('\n', ' '))
        data = pd.DataFrame.from_records(list(query_results), columns=[x for x in list(query_results.keys())])
        return data

    def get_form_factors(self):
        query = \
            """
            SELECT form_factor AS name
            FROM static_new.product
            WHERE size IS NOT NULL AND delete_date IS NULL;
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


if __name__ == '__main__':
    kpis_list = CCRUKPIS()
    kpis_list.validate_and_transform()
