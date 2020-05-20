# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
import json

from openpyxl import load_workbook
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.Queries import Queries
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from datetime import datetime

__author__ = 'Israel'


class Consts(object):

    KPI_TYPE = 'type'
    KPI_CLIENT_NAME = 'client_name'
    STATIC_KPI_LVL_2 = 'static.kpi_level_2_configuration'
    BINARY_FIELDS = ['session_relevance', 'scene_relevance', 'planogram_relevance',
                     'live_session_relevance', 'live_scene_relevance', 'is_percent']
    SALMON = '#FA8072'
    LIME = '#00FF00'
    BLUE = '#87CEFA'


class AddKPIs(object):
    def __init__(self, project_name, template_path=None):
        self.project_name = project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.template_path = self.get_template_path(template_path)
        self.template_data = pd.read_excel(self.template_path)
        self.kpi_counter = 0
        self.insert_queries = []

    @staticmethod
    def get_template_path(template_path):
        return template_path if template_path is not None else os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                            'new_tables_template.xlsx')

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def add_kpis_from_template(self):
        self.insert_into_kpi_lvl_2()

    def insert_into_kpi_lvl_2(self):
        for i, row in self.template_data.iterrows():
            attributes = self.create_attributes_dict(row)
            query = insert(attributes, Consts.STATIC_KPI_LVL_2)
            self.insert_queries.append(query)
        merged_queries = self.merge_insert_queries()
        # print merged_queries
        self.commit_to_db(merged_queries)

    def create_attributes_dict(self, kpi_row):
        attributes_dict = {'kpi_level_2_fk': {0: kpi_row['kpi_level_2_fk']},
                           'start_date': {0: str(kpi_row['start_date'].date())},
                           'end_date': {0: str(kpi_row['end_date'].date())},
                           'kpi_execution_params': {0: json.dumps({'depends_on': [kpi_row['kpi_execution_params']]}) \
                               if str(kpi_row['kpi_execution_params']) != 'nan' else
                           json.dumps({'depends_on': []})},
                           'creation_date': {0: str(datetime.now().date())},
                           'kpi_params': {0: json.dumps({})},
                           'kpi_configration_params': {0: json.dumps({'kpi_type': kpi_row['type']})}
                           }
        return attributes_dict

    def merge_insert_queries(self):
        query_groups = {}
        for query in self.insert_queries:
            if not query:
                continue
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        return merged_queries

    def commit_to_db(self, queries):
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        for query in queries:
            try:
                cur.execute(query)
                self.rds_conn.db.commit()
                print 'kpis were added to the db'
            except Exception as e:
                print 'kpis were not inserted: {}'.format(repr(e))


"""
Template_path: optional attribute. Default value: 'kpi_factory/DevloperTools/new_tables_template.xlsx'- 
               it has all columns required for the script.
Remove_duplicates: optional attribute. True: if you want the script to get rid of repeating kpi types 
                and insert to DB only one of them. Default value: False
Add_pks: optional attribute.True if you want your kpis to have certain pks (e.g.to differenciate the numbering from OOTB).
        Default value: False
Output: If there are no errors, the kpis are added to the DB. If there are errors, the template file with highlighted erroneous 
cells is saved to '/tmp'.
Validations: all validations are in validate_template() function: 
            (1) check if there are kpis with the same names in DB - errors colored red
            (2) check if some of the values that are meant to be binary are not binary in input file - errors colored lime
            (3) check if some kpi lines in input file repeat - errors colored blue
            If you want to skip some validations, you can comment out invocation of the respective validation function.
"""

if __name__ == '__main__':
    LoggerInitializer.init('test')
    Config.init()
    project_name = 'pepsicouk-sand'
    template_path = '/home/natalyak/Desktop/PepsicoUK/RollOut/secondary.xlsx'
    AddKPIs(project_name, template_path=template_path).add_kpis_from_template()
