# - *- coding: utf- 8 - *-
import json
import os
import sys

import pandas as pd

sys.path.append('.')

__author__ = 'urid'


class MARSRU_SANDMARSRUJsonGenerator:
    def __init__(self, project):
        self.project = project
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {}

    def create_template_json(self, file_name, year_filter=None):
        file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        file_json = json.loads(output)
        final_json = []
        for fj in file_json:
            if year_filter and fj.get("Year"):
                if str(int(fj.get("Year"))) == year_filter:
                    final_json.append(fj)
            else:
                final_json.append(fj)
        self.project_kpi_dict['kpi_data'].append({'marsru-kpi': final_json})

    def create_targets_json(self, file_name, key, sheetnames=None):
        if sheetnames and type(sheetnames) is list:
            self.project_kpi_dict[key] = {}
            for sheetname in sheetnames:
                self.project_kpi_dict[key][sheetname] = json.loads(pd.read_excel(os.path.join(self.base_path, file_name),
                                                                                 sheetname=str(sheetname))
                                                                   .to_json(orient='records'))
        else:
            self.project_kpi_dict[key] = json.loads(pd.read_excel(os.path.join(self.base_path, file_name))
                                                    .to_json(orient='records'))









