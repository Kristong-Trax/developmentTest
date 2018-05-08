# - *- coding: utf- 8 - *-
import json
import os
import sys
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import xlrd

sys.path.append('.')

__author__ = 'ortalk'
# BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KC_US_KPIS.xlsx')


class KCUSJsonGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {'project': self.project, 'author': 'ortalk'}

    def create_json(self, file_name):
        # file_input = pd.read_csv(self.path + file_name, encoding='utf-8')
        # file_input = pd.read_excel(self.path + file_name)
        file_input1 = pd.read_excel(os.path.join(self.base_path, file_name),sheetname='Simple KPIs')
        file_input2 = pd.read_excel(os.path.join(self.base_path, file_name), sheetname='Block Together')
        file_input3 = pd.read_excel(os.path.join(self.base_path, file_name), sheetname='Relative Position')
        output1 = file_input1.to_json(orient='records')
        output2 = file_input2.to_json(orient='records')
        output3 = file_input3.to_json(orient='records')
        # json_acceptable_string = output.replace("'", "\"")
        # final_json = json.loads(json_acceptable_string)
        final_json1 = json.loads(output1)
        final_json2 = json.loads(output2)
        final_json3 = json.loads(output3)
        self.project_kpi_dict.update({'Simple KPIs':final_json1})
        self.project_kpi_dict.update({'Block Together':final_json2})
        self.project_kpi_dict.update({'Relative Position' : final_json3})

        # with open('/home/uri/dev/theGarage/Trax/Analytics/Calculation/Ps/CCRU/Utils/data.txt', 'w') as outfile:
        #     json.dump(final_json, outfile)

        return

    # @staticmethod
    # def get_json_data(sheet_name=None, skiprows=0):
    #     """
    #     This function gets a file's path and extract its content into a JSON.
    #     """
    #     data = {}
    #     if sheet_name:
    #         sheet_names = [sheet_name]
    #     else:
    #         sheet_names = xlrd.open_workbook().sheet_names()
    #     for sheet_name in sheet_names:
    #         try:
    #             output = pd.read_excel(BASE_PATH, sheetname=sheet_name, skiprows=skiprows)
    #         except xlrd.biffh.XLRDError:
    #             Log.warning('Sheet name {} doesn\'t exist'.format(sheet_name))
    #             return None
    #         output = output.to_json(orient='records')
    #         output = json.loads(output)
    #         data[sheet_name] = output
    #     if sheet_name:
    #         data = data[sheet_name]
    #     elif len(data.keys()) == 1:
    #         data = data[data.keys()[0]]
    #     return data

    def create_targets_json(self, file_name, key, sheet_name=None):
        if sheet_name:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name), sheetname=str(sheet_name))
        else:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        final_json = json.loads(output)
        self.project_kpi_dict[key] = final_json

        return









