# - *- coding: utf- 8 - *-
import json
import os
import sys
import pandas as pd

sys.path.append('.')


__author__ = 'urid'


class MARSRU2_SANDJSONGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {}

    def create_template_json(self, file_name, key, sheet_names=None):
        if sheet_names:
            if type(sheet_names) is list:
                self.project_kpi_dict[key] = {}
                for sheet_name in sheet_names:
                    self.project_kpi_dict[key][sheet_name] = \
                        json.loads(pd.read_excel(os.path.join(self.base_path, file_name), sheet_name=str(sheet_name))
                                   .to_json(orient='records'))
            else:
                self.project_kpi_dict[key] = \
                    json.loads(pd.read_excel(os.path.join(self.base_path, file_name), sheet_name=str(sheet_names))
                               .to_json(orient='records'))
        else:
            self.project_kpi_dict[key] = \
                json.loads(pd.read_excel(os.path.join(self.base_path, file_name))
                           .to_json(orient='records'))
