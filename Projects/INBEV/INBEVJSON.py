# - *- coding: utf- 8 - *-
import json
import os
import sys

import pandas as pd

sys.path.append('.')

__author__ = 'urid'


class JsonGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {'project': self.project, 'author': 'urid', 'golden_shelves': '', 'kpi_data': []}

    def create_json(self, file_name):
        file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        final_json = json.loads(output)
        self.project_kpi_dict['kpi_data'].append({'marsru-kpi': final_json})
        return

    def create_targets_json(self, file_name, key, sheet_name=None):
        if sheet_name:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name), sheetname=str(sheet_name))
        else:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        final_json = json.loads(output)
        self.project_kpi_dict[key] = final_json
        return