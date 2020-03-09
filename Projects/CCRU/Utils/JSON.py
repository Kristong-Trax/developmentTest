# - *- coding: utf- 8 - *-
import os
import sys
import json
import pandas as pd


__author__ = 'urid'


sys.path.append('.')


class CCRUJsonGenerator:
    def __init__(self):
        self.base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.project_kpi_dict = {}

    def create_kpi_source(self, file_name, pos_kpi_set):
        input_df = pd.read_excel(os.path.join(self.base_path, file_name), sheet_name=None)

        output = pd.DataFrame()
        for source_key in input_df.keys():
            source_df = input_df[source_key]
            source_df['SOURCE'] = source_key
            output = output.append(source_df[source_df['POS'] == pos_kpi_set])

        output = output.to_json(orient='records')
        output = json.loads(output)

        return output

    def create_kpi_data_json(self, kpi_data, file_name, sheet_name=None, pos_kpi_set_name=None):
        sheet_name = str(sheet_name) if sheet_name else None
        if sheet_name:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name), sheet_name=sheet_name)
            if 'PoS name' in file_input.columns and pos_kpi_set_name:
                file_input = file_input[file_input['PoS name'] == pos_kpi_set_name]
        else:
            file_input = pd.read_excel(os.path.join(self.base_path, file_name))
        output = file_input.to_json(orient='records')
        final_json = json.loads(output)

        if kpi_data == 'kpi_data':
            if not self.project_kpi_dict.get(kpi_data):
                self.project_kpi_dict[kpi_data] = []
            final_json = self.remove_none_values_from_json(final_json)
            final_json_main = []
            final_json_hidden = []
            for fj in final_json:
                if fj.get("KPI Type") != "Hidden":
                    if fj.get("Children"):
                        fj.update({"Children List": map(int, str(fj.get("Children")).strip().replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))})
                    else:
                        fj.update({"Children List": []})
                    final_json_main.append(fj)
                else:
                    if fj.get("Children"):
                        fj.update({"Children List": map(int, str(fj.get("Children")).strip().replace(" ", "").replace(",", "\n").replace("\n\n", "\n").split("\n"))})
                    else:
                        fj.update({"Children List": []})
                    final_json_hidden.append(fj)
            children_hidden = {"SESSION LEVEL": [], "SCENE LEVEL": []}
            for fj in final_json_hidden:
                if fj.get("Type") == "SESSION LEVEL":
                    children_hidden["SESSION LEVEL"] += fj.get("Children List")
                elif fj.get("Type") == "SCENE LEVEL":
                    children_hidden["SCENE LEVEL"] += fj.get("Children List")
                else:
                    continue
            store_type_dict = {0: final_json_main, 1: final_json_hidden, 2: children_hidden}
            self.project_kpi_dict[kpi_data].append(store_type_dict)

        else:
            self.project_kpi_dict[kpi_data] = final_json

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
