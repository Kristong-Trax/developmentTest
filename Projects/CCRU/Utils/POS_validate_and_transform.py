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
POS_LIST_FILE = 'PoS 2020 List.xlsx'
POS_ALL_FILE = 'PoS 2020 ALL.xlsx'

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

    'Result Format',
    'Target',
    'target_min',
    'target_max',
    'score_func',
    'score_min',
    'score_max',

    'depends on',

    'Type',
    'Values',

    'Products to exclude',
    'Size',
    'Form Factor',
    'Brand',
    'Sub category',
    'Product Category',
    'Manufacturer',

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

ALLOWED_SYMBOLS_KPI_NAME_ENG = ' ' \
                            '0123456789' \
                            'ABCDEFGHIJKLMNOPQRSTUVWXWZ' \
                            'abcdefghigklmnopqrstuvwxyz' \
                            '%&()[]-_+/<>.:'
ALLOWED_SYMBOLS_KPI_NAME_RUS = u' ' \
                               u'0123456789' \
                               u'ABCDEFGHIJKLMNOPQRSTUVWXWZ' \
                               u'abcdefghigklmnopqrstuvwxyz' \
                               u'%&()[]-_+/<>.:' \
                               u'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ' \
                               u'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
ALLOWED_FORMULAS = \
    [
        'check_number_of_scenes_with_facings_target',
        'each SKU hits facings target',
        'facings TCCC/40',
        'Group',
        'Lead SKU',
        'number of atomic KPI Passed',
        'number of atomic KPI Passed on the same scene',
        'number of coolers with facings target and fullness target',
        'number of doors of filled Coolers',
        'number of doors with more than Target facings',
        'number of facings',
        'number of pure Coolers',
        'number of scenes',
        'number of SKU per Door RANGE TOTAL',
        'number of sub atomic KPI Passed',
        'Scenes with no tagging',
        # 'Share of CCH doors which do not have FC packs',
        'Share of CCH doors which have 98% TCCC facings',
        'SOS',
        'sum of atomic KPI result',
        'Weighted Average',
    ]

ALLOWED_CATEGORY_KPI_TYPES = \
    [
        'Availability',
        'SOS'
    ]

ALLOWED_LOGICAL_OPERATORS = \
    [
        'OR',
        'AND',
        'MAX',
        'SUM'
    ]


class CCRUKPIS:

    def __init__(self):

        self.project = PROJECT

        self.rds_conn = OrmSession(self.project, writable=False)

        self.kpi_names = self.get_kpi_names()
        self.kpi_level_2_names = self.get_kpi_level_2_names()['type'].unique().tolist()
        self.categories = self.get_categories()['name'].unique().tolist()

    @staticmethod
    def xl_col_num_to_ltr(num):
        letters = ''
        while num:
            mod = (num - 1) % 26
            letters += chr(mod + 65)
            num = (num - 1) // 26
        return ''.join(reversed(letters))

    def validate_and_transform(self):

        pos_list_df = pd.read_excel(os.path.join(POS_PATH, POS_LIST_FILE))
        pos_list_df = pos_list_df.where((pd.notnull(pos_list_df)), None)

        pos_all = pd.DataFrame()
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

            # fixing internal extra spaces in KPI names Eng
            field_name = 'KPI name Eng'
            for i, r in pos.iterrows():
                kpi_name_stripped = unicode(r[field_name])\
                    .replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip()
                pos.loc[i, field_name] = kpi_name_stripped

            # fixing internal extra spaces in KPI names Rus
            field_name = 'KPI name Rus'
            for i, r in pos.iterrows():
                kpi_name_stripped = unicode(r[field_name])\
                    .replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip()
                pos.loc[i, field_name] = kpi_name_stripped

            # checking KPI ID
            field_name = 'KPI ID'
            error_name = '01_ERROR {} EMPTY OR DUPLICATE'.format(field_name)
            duplicates = pos[[field_name, 'Sorting']].groupby(field_name).count()\
                .reset_index().rename(columns={'Sorting': 'count'})
            duplicates_list = duplicates[duplicates['count'] > 1][field_name].unique().tolist()
            if duplicates_list:
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[pos[field_name].isin(duplicates_list), error_name] = \
                    pos[pos[field_name].isin(duplicates_list)][field_name].astype(unicode)
                structure_is_ok = False

            # checking level
            pos['00_checked'] = None
            field_name = 'level'
            error_name = '02_ERROR {} INCORRECT'.format(field_name)
            incorrect_levels_mask = (pos['level'].isnull()) | ~(pos['level'].isin([1, 2, 3, 4]))
            incorrect_levels = pos[incorrect_levels_mask]['level'].astype(unicode)
            if len(incorrect_levels):
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[incorrect_levels_mask, 'level'] = incorrect_levels
                structure_is_ok = False
            for i, r0 in pos[pos['Children'].notnull()].iterrows():
                if r0['level'] == 1 and r0['Parent'] is None:
                    pos.loc[i, '00_checked'] = 1
                if r0['level'] == 4:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r0[field_name])
                    structure_is_ok = False
                level = None
                for j, r1 in pos[pos['Parent'] == r0['KPI ID']].iterrows():
                    pos.loc[j, '00_checked'] = 1
                    level = r1['level'] if level is None else level
                    if r1['level'] != level \
                            or not (level in [1, 2] and r0['level'] == 1 or
                                    level in [3] and r0['level'] == 2 or
                                    level in [4] and r0['level'] == 3):
                        if error_name not in pos.columns:
                            pos[error_name] = None
                        pos.loc[j, error_name] = unicode(r1[field_name])
                        structure_is_ok = False

            # checking loose Parent
            field_name = 'Parent'
            error_name = '03_ERROR {} LOOSE'.format(field_name)
            if len(pos[pos['00_checked'].isnull()]) > 0:
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[pos['00_checked'].isnull(), error_name] = \
                    pos[pos['00_checked'].isnull()][field_name].astype(unicode)
                structure_is_ok = False

            # checking length of KPI name Eng
            field_name = 'KPI name Eng'
            error_name = '04_ERROR {} TOO SHORT'.format(field_name)
            for i, r in pos.iterrows():
                if len(unicode(r[field_name])) < 5:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])
                    contents_is_ok = False

            # checking length of KPI nam Rus
            field_name = 'KPI name Rus'
            error_name = '05_ERROR {} TOO SHORT'.format(field_name)
            for i, r in pos.iterrows():
                if len(unicode(r[field_name])) < 5:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])
                    contents_is_ok = False

            # checking for restricted symbols in KPI names Eng
            field_name = 'KPI name Eng'
            error_name = '06_ERROR {} RESTRICTED SYMBOLS'.format(field_name)
            for i, r in pos.iterrows():
                if any((s not in ALLOWED_SYMBOLS_KPI_NAME_ENG) for s in unicode(r[field_name])):
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])
                    contents_is_ok = False

            # checking for restricted symbols in KPI names Rus
            field_name = 'KPI name Rus'
            error_name = '07_ERROR {} RESTRICTED SYMBOLS'.format(field_name)
            for i, r in pos.iterrows():
                if any((s not in ALLOWED_SYMBOLS_KPI_NAME_RUS) for s in unicode(r[field_name])):
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])
                    contents_is_ok = False

            # checking for duplicate KPI name Eng
            field_name = 'KPI name Eng'
            error_name = '08_ERROR {} DUPLICATE'.format(field_name)
            duplicates = pos[[field_name, 'Sorting']].groupby(field_name).count()\
                .reset_index().rename(columns={'Sorting': 'count'})
            duplicates_list = duplicates[duplicates['count'] > 1][field_name].unique().tolist()
            if duplicates_list:
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[pos[field_name].isin(duplicates_list), error_name] = \
                    pos[pos[field_name].isin(duplicates_list)][field_name].astype(unicode)
                contents_is_ok = False

            # checking total KPI Weight == 1.0
            field_name = 'KPI Weight'
            error_name = '09_ERROR {} TOTAL != 1.0'.format(field_name)
            if pos[field_name].sum() != 1.0:
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[error_name] = unicode(pos[field_name])
                contents_is_ok = False

            # checking KPI Weight for decimals
            field_name = 'KPI Weight'
            error_name = '10_ERROR {} DECIMALS > 6 DIGITS'.format(field_name)
            weights = pos['KPI Weight'].astype(unicode).str.split('.', 1, True)
            weights[2] = weights[1].str.len()
            incorrect_precision_mask = (weights[2] > 2)
            if len(incorrect_precision_mask) > 0:
                if error_name not in pos.columns:
                    pos[error_name] = None
                pos.loc[incorrect_precision_mask, error_name] = \
                    pos[incorrect_precision_mask][field_name].astype(unicode)
                contents_is_ok = False

            # checking Category KPI Type
            field_name = 'Category KPI Type'
            error_name = '11_ERROR {} NOT ALLOWED'.format(field_name)
            for i, r in pos[pos[field_name].notnull()].iterrows():
                if r[field_name] not in ALLOWED_CATEGORY_KPI_TYPES:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])

            # checking Category KPI Value
            field_name = 'Category KPI Value'
            error_name = '12_ERROR {} NOT ALLOWED'.format(field_name)
            for i, r in pos[pos['Category KPI Type'].notnull()].iterrows():
                if r[field_name] not in self.categories:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = unicode(r[field_name])

            # checking Formula
            field_name = 'Formula'
            error_name = '12_ERROR {} NOT ALLOWED'.format(field_name)
            for i, r in pos.iterrows():
                if r[field_name] not in ALLOWED_FORMULAS:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = r[field_name]

            # checking Logical Operator
            field_name = 'Logical Operator'
            error_name = '13_ERROR {} NOT ALLOWED'.format(field_name)
            for i, r in pos[pos[field_name].notnull()].iterrows():
                if r[field_name] not in ALLOWED_LOGICAL_OPERATORS:
                    if error_name not in pos.columns:
                        pos[error_name] = None
                    pos.loc[i, error_name] = r[field_name]

            if structure_is_ok:
                # building old kpi structure columns
                pos['99_kpi_set_name'] = None
                pos['99_kpi_name'] = None
                pos['99_kpi_weight'] = None
                pos['99_kpi_atomic_name'] = None
                pos['99_kpi_atomic_display_name'] = None
                pos['99_kpi_atomic_display_name_rus'] = None
                pos['99_kpi_atomic_wight'] = None
                for i, r in pos[(pos['level'] == 2) & (pos['Children'].isnull())].iterrows():
                    pos.loc[i, '99_kpi_set_name'] = pos_name
                    pos.loc[i, '99_kpi_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_weight'] = r['99_weight']
                    pos.loc[i, '99_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '99_kpi_atomic_wight'] = r['99_weight']
                for i, r in pos[pos['level'] == 3].iterrows():
                    pos.loc[i, '99_kpi_set_name'] = pos_name
                    pos.loc[i, '99_kpi_name'] = pos[pos['KPI ID'] == r['Parent']]['KPI name Eng'].values[0]
                    pos.loc[i, '99_kpi_weight'] = pos[pos['KPI ID'] == r['Parent']]['99_weight'].values[0]
                    pos.loc[i, '99_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '99_kpi_atomic_wight'] = r['99_weight']
                for i, r in pos[pos['level'] == 4].iterrows():
                    pos.loc[i, '99_kpi_set_name'] = pos_name
                    pos.loc[i, '99_kpi_name'] = pos[pos['KPI ID'] == r['Parent']]['99_kpi_name'].values[0]
                    pos.loc[i, '99_kpi_weight'] = pos[pos['KPI ID'] == r['Parent']]['99_kpi_weight'].values[0]
                    pos.loc[i, '99_kpi_atomic_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name'] = r['KPI name Eng']
                    pos.loc[i, '99_kpi_atomic_display_name_rus'] = r['KPI name Rus']
                    pos.loc[i, '99_kpi_atomic_wight'] = r['99_weight']

                # calculating weights for all levels
                pos['99_weight'] = None
                pos.loc[(pos['level'] == 4) & (pos['KPI Weight'].notnull()), '99_weight'] = \
                    pos[(pos['level'] == 4) & (pos['KPI Weight'].notnull())]['KPI Weight']
                pos.loc[(pos['level'] == 3) & (pos['KPI Weight'].notnull()), '99_weight'] = \
                    pos[(pos['level'] == 3) & (pos['KPI Weight'].notnull())]['KPI Weight']
                pos.loc[(pos['level'] == 2) & (pos['KPI Weight'].notnull()), '99_weight'] = \
                    pos[(pos['level'] == 2) & (pos['KPI Weight'].notnull())]['KPI Weight']
                for i, r in pos[(pos['level'] == 3) & (pos['KPI Weight'].isnull()) & (pos['Children'].notnull())]\
                        .iterrows():
                    if len(pos[(pos['Parent'] == r['KPI ID']) & (pos['KPI Weight'].notnull())]) > 0:
                        pos.loc[i, '99_weight'] = pos[pos['Parent'] == r['KPI ID']]['KPI Weight'].sum()
                for i, r in pos[(pos['level'] == 2) & (pos['KPI Weight'].isnull()) & (pos['Children'].notnull())]\
                        .iterrows():
                    if len(pos[(pos['Parent'] == r['KPI ID']) & pos['KPI Weight'].notnull()]) > 0:
                        pos.loc[i, '99_weight'] = pos[pos['Parent'] == r['KPI ID']]['KPI Weight'].sum()

                # checking POS name in the DB
                field_name = 'PoS name'
                error_name = '08_ERROR PoS name NOT IN DB kpi_set'
                if pos_name not in self.kpi_names['kpi_set_name'].unique().tolist():
                    pos[error_name] = pos[field_name]

                # checking KPI name Eng names in the DB kpis
                field_name = 'KPI name Eng'
                error_name = '09_ERROR KPI name Eng NOT IN DB kpi'
                check_list = self.kpi_names[self.kpi_names['kpi_set_name'] == pos_name]['kpi_name'].unique().tolist()
                for i, r in pos[pos['level'] == 2].iterrows():
                    if r[field_name] not in check_list:
                        if error_name not in pos.columns:
                            pos[error_name] = None
                        pos.loc[i, error_name] = r[field_name]

                # checking KPI name Eng names in the DB atomic_kpi
                field_name = 'KPI name Eng'
                error_name = '10_ERROR KPI name Eng NOT IN DB atomic_kpi'
                for i, r in pos[pos['level'] in [3, 4]].iterrows():
                    check_list = self.kpi_names[(self.kpi_names['kpi_set_name'] == r['99_kpi_set_name']) &
                                                (self.kpi_names['kpi_name'] == r['99_kpi_name'])][
                        'atomic_kpi_name'].unique().tolist()
                    if r[field_name] not in check_list:
                        if error_name not in pos.columns:
                            pos[error_name] = None
                        pos.loc[i, error_name] = r[field_name]

                # checking KPI name Eng names in the DB kpi_level_2
                field_name = 'KPI name Eng'
                error_name = '11_ERROR KPI name Eng NOT IN DB kpi_level_2'
                check_list = self.kpi_level_2_names
                for i, r in pos.iterrows():
                    if r[field_name] not in check_list:
                        if error_name not in pos.columns:
                            pos[error_name] = None
                        pos.loc[i, error_name] = r[field_name]

            pos_all = pos_all.append(pos, ignore_index=True)

        pos_cols = POS_COLUMNS + sorted(list(set(pos_all.columns) - set(POS_COLUMNS)))

        pos_all = pos_all.reindex(columns=pos_cols)
        writer = pd.ExcelWriter(os.path.join(POS_OUTPUT_PATH, POS_ALL_FILE), engine='xlsxwriter')
        pos_all.to_excel(writer, sheet_name='Sheet1', index=False)

        # pos_rgt = self.num_to_col_letters(len(pos_df.columns))
        # pos_btm = len(pos_df)
        #
        # pos_xl = xl.Workbook(os.path.join(POS_OUTPUT_PATH, 'PoS 2020 ALL.xlsx')).get_worksheet_by_name('Sheet1')
        # ws = pos_xl
        # ws.set_row(30)
        #
        # kpis_output = pd.DataFrame()
        # for file_name in files:
        #     kpis_input = pd.read_excel(os.path.join(path, file_name))
        #     kpis_input = kpis_input.where((pd.notnull(kpis_input)), 0)
        #     kpis = kpis_input[~(kpis_input['KPI Type'] == 'Hidden') &
        #                       ~(kpis_input['level'] == 1)][
        #         ['KPI ID', 'KPI name Eng', 'KPI name Rus', 'KPI Weight', 'level', 'Parent']]
        #     for i, kpi in kpis[kpis['level'] == 2].iterrows():
        #         if kpis[kpis['Parent'] == kpi['KPI ID']].empty:
        #             kpis.loc[i, 'Parent'] = kpi['KPI ID']
        #         kpi_weight = 0
        #         for j, atomic in kpis[kpis['Parent'] == kpi['KPI ID']].iterrows():
        #             atomic_weight = 0
        #             for k, subatomic in kpis[kpis['Parent'] == atomic['KPI ID']].iterrows():
        #                 kpis.loc[k, 'Parent'] = atomic['Parent']
        #                 subatomic_weight = 0
        #                 if subatomic['KPI Weight']:
        #                     subatomic_weight = round(subatomic['KPI Weight'], 6)
        #                 kpis.loc[k, 'KPI Weight'] = subatomic_weight
        #                 atomic_weight += subatomic_weight
        #             if atomic['KPI Weight']:
        #                 atomic_weight = round(atomic['KPI Weight'], 6)
        #             kpis.loc[j, 'KPI Weight'] = atomic_weight
        #             kpi_weight += atomic_weight
        #         if kpi['KPI Weight']:
        #             kpi_weight = round(kpi['KPI Weight'], 6)
        #         kpis.loc[i, 'KPI Weight'] = kpi_weight
        #
        #     kpis = kpis[kpis['level'] == 2].merge(
        #         kpis, how='left', left_on='KPI ID', right_on='Parent', suffixes=['_2', '_3'])
        #     kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
        #                       how='left',
        #                       left_on='Parent_2', right_on='KPI ID', suffixes=['_2', '_12'])
        #     kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
        #                       how='left',
        #                       left_on='Parent', right_on='KPI ID', suffixes=['_12', '_11'])
        #     kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
        #                       how='left',
        #                       left_on='Parent_11', right_on='KPI ID', suffixes=['_11', '_10'])
        #
        #     kpis['KPI Set'] = file_name.replace('.xlsx', '')
        #     kpis_output = kpis_output.append(kpis, ignore_index=True)
        #
        # writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_PoS.xlsx'), engine='xlsxwriter')
        # kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)
        #
        # kpis_output = pd.DataFrame()
        # kpis_values = []
        # for file_name in files:
        #     kpis_input = pd.read_excel(os.path.join(path, file_name))
        #     kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
        #     kpis = kpis_input[(kpis_input['KPI Type'] == 'Hidden')
        #                       ][['KPI name Eng', 'KPI name Rus']]
        #     kpis['KPI name Rus'] = kpis['KPI name Eng']
        #     kpis_output = kpis_output.append(kpis, ignore_index=True)
        #
        #     kpis = kpis_input[(kpis_input['KPI Type'] == 'Hidden')][['Values']]
        #     for v in kpis['Values'].tolist():
        #         if v:
        #             kpis_values.extend(v.split('\n'))
        #
        # kpis_output = kpis_output.drop_duplicates()
        # kpis_values = pd.DataFrame(kpis_values).drop_duplicates()
        #
        # writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_Hidden.xlsx'), engine='xlsxwriter')
        # kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)
        # kpis_values.to_excel(writer, sheet_name='Sheet2', index=False)
        #
        # kpis_output = pd.DataFrame()
        # for file_name in files:
        #     kpis_input = pd.read_excel(os.path.join(path, file_name))
        #     kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
        #     kpis = kpis_input[(kpis_input['KPI Type'] != 'Hidden')
        #                       ][['KPI name Eng', 'KPI name Rus']]
        #     kpis_output = kpis_output.append(kpis, ignore_index=True)
        #
        # kpis_output = kpis_output.drop_duplicates()
        #
        # writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_Level_2.xlsx'), engine='xlsxwriter')
        # kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)

        # scene_types = pd.read_excel(os.path.join(path, '_scene_types.xlsx'))
        # scene_types = scene_types['name'].unique().tolist()
        #
        # kpis_output = pd.DataFrame()
        # for file_name in files:
        #     kpis_input = pd.read_excel(os.path.join(path, file_name))
        #     kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
        #     kpis_input['error'] = ''
        #     for i, row in kpis_input.iterrows():
        #         for scene_type in row['Scenes to include'].split(', ') if row['Scenes to include'] else []:
        #             if scene_type not in scene_types:
        #                 kpis_input.loc[i, 'error'] += scene_type + ', '
        #     kpis_input['POS'] = file_name
        #     kpis_output = kpis_output.append(kpis_input[['POS', 'Scenes to include', 'error']], ignore_index=True)
        #
        # kpis_output = kpis_output[kpis_output['error'] != '']
        #
        # writer = pd.ExcelWriter(os.path.join(path, '_scene_types_error.xlsx'), engine='xlsxwriter')
        # kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)

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
                SELECT type 
                FROM static.kpi_level_2
                WHERE initiated_by='Entity';
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

if __name__ == '__main__':
    kpis_list = CCRUKPIS()
    kpis_list.validate_and_transform()
