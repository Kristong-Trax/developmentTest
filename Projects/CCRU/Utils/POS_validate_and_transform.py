# - *- coding: utf- 8 - *-
import os
import sys
import json
import pandas as pd
import xlsxwriter as xl


__author__ = 'sergey'


sys.path.append('.')

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
    'score_func',
    'score_min',
    'score_max',

    'Result Format',
    'Target',
    'target_min',
    'target_max',

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


class CCRUKPIS:

    def __init__(self):
        pass

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
        for i, row in pos_list_df.iterrows():
            pos_name = row['POS']
            file_in = row['File_in']
            sheet_in = row['Sheet_in'] if row['Sheet_in'] else 0
            file_out = row['File_out']
            sheet_out = row['Sheet_out'] if row['Sheet_out'] else 'Sheet1'

            pos_input = pd.read_excel(os.path.join(POS_PATH_INPUT, file_in), sheet_name=sheet_in)
            pos_input['PoS name'] = pos_name

            pos_input['File_out'] = file_out
            pos_input['Sheet_out'] = sheet_out

            pos_all = pos_all.append(pos_input, ignore_index=True)

        pos_cols = POS_COLUMNS + list(set(pos_all.columns) - set(POS_COLUMNS))

        pos_all = pos_all.reindex(columns=pos_cols)
        pos_all = pos_all.where((pd.notnull(pos_all)), None)
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



if __name__ == '__main__':
    kpis_list = CCRUKPIS()
    kpis_list.validate_and_transform()
