# - *- coding: utf- 8 - *-
import json
import os
import sys
import pandas as pd

sys.path.append('.')


__author__ = 'urid'


class MARSRU_PRODJSONGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {}

    def create_template_json(self, file_name, key, sheetnames=None):
        if sheetnames:
            if type(sheetnames) is list:
                self.project_kpi_dict[key] = {}
                for sheetname in sheetnames:
                    self.project_kpi_dict[key][sheetname] = \
                        json.loads(pd.read_excel(os.path.join(self.base_path, file_name), sheetname=str(sheetname))
                                   .to_json(orient='records'))
            else:
                self.project_kpi_dict[key] = \
                    json.loads(pd.read_excel(os.path.join(self.base_path, file_name), sheetname=str(sheetnames))
                               .to_json(orient='records'))
        else:
            self.project_kpi_dict[key] = \
                json.loads(pd.read_excel(os.path.join(self.base_path, file_name))
                           .to_json(orient='records'))
