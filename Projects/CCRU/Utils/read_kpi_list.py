# - *- coding: utf- 8 - *-
import os
import sys
import json
import pandas as pd


__author__ = 'sergey'


sys.path.append('.')


class CCRUKPIS:

    def __init__(self):
        pass

    def create_kpis_list(self):

        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data/KPIs_2019')
        files = [
            'PoS 2019 - FT - CAP.xlsx',
            'PoS 2019 - FT NS - CAP.xlsx',
            'PoS 2019 - FT NS - REG.xlsx',
            'PoS 2019 - FT - REG.xlsx',
            'PoS 2019 - IC Canteen - EDU.xlsx',
            'PoS 2019 - IC Canteen - OTH.xlsx',
            'PoS 2019 - IC HoReCa BarTavernClub - CAP.xlsx',
            'PoS 2019 - IC HoReCa BarTavernClub - REG.xlsx',
            'PoS 2019 - IC HoReCa RestCafeTea - CAP.xlsx',
            'PoS 2019 - IC HoReCa RestCafeTea - REG.xlsx',
            'PoS 2019 - IC Petroleum - CAP.xlsx',
            'PoS 2019 - IC Petroleum - REG.xlsx',
            'PoS 2019 - IC QSR.xlsx',
            'PoS 2019 - MT Conv Big - CAP.xlsx',
            'PoS 2019 - MT Conv Big - REG.xlsx',
            'PoS 2019 - MT Conv Small - CAP.xlsx',
            'PoS 2019 - MT Conv Small - REG.xlsx',
            'PoS 2019 - MT Hypermarket - CAP.xlsx',
            'PoS 2019 - MT Hypermarket - REG.xlsx',
            'PoS 2019 - MT Supermarket - CAP.xlsx',
            'PoS 2019 - MT Supermarket - REG.xlsx',
        ]
        kpis_output = pd.DataFrame()
        for file_name in files:
            kpis_input = pd.read_excel(os.path.join(path, file_name))
            kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
            kpis = kpis_input[~(kpis_input['KPI Type'] == 'Hidden') &
                              ~(kpis_input['level'] == 1)][
                ['KPI ID', 'KPI name Eng', 'KPI name Rus', 'KPI Weight', 'level', 'Parent']]
            for i, kpi in kpis[kpis['level'] == 2].iterrows():
                if kpis[kpis['Parent'] == kpi['KPI ID']].empty:
                    kpis.loc[i, 'Parent'] = kpi['KPI ID']
                kpi_weight = 0
                for j, atomic in kpis[kpis['Parent'] == kpi['KPI ID']].iterrows():
                    atomic_weight = 0
                    for k, subatomic in kpis[kpis['Parent'] == atomic['KPI ID']].iterrows():
                        kpis.loc[k, 'Parent'] = atomic['Parent']
                        subatomic_weight = 0
                        if subatomic['KPI Weight']:
                            subatomic_weight = round(subatomic['KPI Weight'], 6)
                        kpis.loc[k, 'KPI Weight'] = subatomic_weight
                        atomic_weight += subatomic_weight
                    if atomic['KPI Weight']:
                        atomic_weight = round(atomic['KPI Weight'], 6)
                    kpis.loc[j, 'KPI Weight'] = atomic_weight
                    kpi_weight += atomic_weight
                if kpi['KPI Weight']:
                    kpi_weight = round(kpi['KPI Weight'], 6)
                kpis.loc[i, 'KPI Weight'] = kpi_weight

            kpis = kpis[kpis['level'] == 2].merge(kpis, how='left', left_on='KPI ID', right_on='Parent', suffixes=['_2', '_3'])
            kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
                              how='left',
                              left_on='Parent_2', right_on='KPI ID', suffixes=['_2', '_12'])
            kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
                              how='left',
                              left_on='Parent', right_on='KPI ID', suffixes=['_12', '_11'])
            kpis = kpis.merge(kpis_input[['KPI ID', 'KPI name Eng', 'KPI name Rus', 'Parent']],
                              how='left',
                              left_on='Parent_11', right_on='KPI ID', suffixes=['_11', '_10'])

            kpis['KPI Set'] = file_name.replace('.xlsx', '')
            kpis_output = kpis_output.append(kpis, ignore_index=True)

        writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_PoS.xlsx'), engine='xlsxwriter')
        kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)

        kpis_output = pd.DataFrame()
        for file_name in files:
            kpis_input = pd.read_excel(os.path.join(path, file_name))
            kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
            kpis = kpis_input[(kpis_input['KPI Type'] == 'Hidden')][['KPI name Eng', 'KPI name Rus']]
            kpis['KPI name Rus'] = kpis['KPI name Eng']
            kpis_output = kpis_output.append(kpis, ignore_index=True)

        kpis_output = kpis_output.drop_duplicates()

        writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_Hidden.xlsx'), engine='xlsxwriter')
        kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)

        kpis_output = pd.DataFrame()
        for file_name in files:
            kpis_input = pd.read_excel(os.path.join(path, file_name))
            kpis_input = kpis_input.where((pd.notnull(kpis_input)), None)
            kpis = kpis_input[(kpis_input['KPI Type'] != 'Hidden')][['KPI name Eng', 'KPI name Rus']]
            kpis_output = kpis_output.append(kpis, ignore_index=True)

        kpis_output = kpis_output.drop_duplicates()

        writer = pd.ExcelWriter(os.path.join(path, 'KPIs_List_Level_2.xlsx'), engine='xlsxwriter')
        kpis_output.to_excel(writer, sheet_name='Sheet1', index=False)


if __name__ == '__main__':
    kpis_list = CCRUKPIS()
    kpis_list.create_kpis_list()

