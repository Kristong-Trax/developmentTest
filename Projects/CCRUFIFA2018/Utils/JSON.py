# - *- coding: utf- 8 - *-
import json
import os
import sys

import pandas as pd
from Trax.Utils.Logging.Logger import Log

sys.path.append('.')

__author__ = 'urid'


class JsonGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {'project': self.project, 'author': 'urid', 'cat_targets_by_region': '', 'kpi_data': []}

    def create_json(self, file_name, level1_name, sheetname=None):
        # file_input = pd.read_csv(self.path + file_name, encoding='utf-8')
        # file_input = pd.read_excel(self.path + file_name)
        file_input = pd.read_excel(os.path.join(self.base_path, file_name), sheetname=sheetname)
        output = file_input.to_json(orient='records')
        # json_acceptable_string = output.replace("'", "\"")
        # final_json = json.loads(json_acceptable_string)
        final_json = json.loads(output)
        final_json = self.remove_none_values_from_json(final_json)
        store_type_dict = {level1_name: final_json}
        self.project_kpi_dict['kpi_data'].append(store_type_dict)
        # with open('/home/uri/dev/theGarage/Trax/Analytics/Calculation/Ps/CCRU/Utils/data.txt', 'w') as outfile:
        #     json.dump(final_json, outfile)

        return

    def create_targets_json(self, file_name):
        file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        final_json = json.loads(output)
        self.project_kpi_dict['cat_targets_by_region'] = final_json
        return

    @staticmethod
    def remove_none_values_from_json(json_data):
        """
        This func removes all the keys whose values are 'None' (meaning, the cells which are empty in the excel
        for any of the headlines).
        This is done so the dict.get(arg1, arg2) function would work - otherwise, it would return None for these
        values and not return the arg2 value instead.
        """
        for i in xrange(len(json_data)):
            for key in json_data[i].keys():
                if json_data[i][key] is None:
                    json_data[i].pop(key)
        return json_data









