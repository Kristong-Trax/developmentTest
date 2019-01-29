# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np

from openpyxl import load_workbook
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.Queries import Queries
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

__author__ = 'Israel'


class Consts(object):

    KPI_TYPE = 'type'
    KPI_CLIENT_NAME = 'client_name'
    STATIC_KPI_LVL_2 = 'static.kpi_level_2'
    STATIC_KPI_VIEW_CONFIG = 'static.kpi_view_configuration'


class AddKPIsToAPI(object):
    def __init__(self, project_name, file_path=None, kpi_list=None, all_existing_kpis=False, kpis_to_exclude=None):
        self.project_name = project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.existing_configurations = self.get_kpi_view_config_api()
        self.template_path = file_path
        self.template_data = pd.read_excel(self.template_path) if self.template_path is not None else None
        self.all_existing_kpis = all_existing_kpis
        self.insert_queries = []
        self.kpi_list = list(set(kpi_list)) if kpi_list is not None else kpi_list
        self.kpis_to_exclude = list(set(kpis_to_exclude)) if kpis_to_exclude is not None else []

    def get_output_file_path(self):
        path_to_list = self.template_path.split('/')
        file_name = path_to_list[len(path_to_list)-1]
        output_path = os.path.join('/tmp', file_name)
        return output_path

    def get_kpi_view_config_api(self):
        query = """ select * from static.kpi_view_configuration where application='API' """
        kpi_config_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_config_data['kpi_level_2_fk'].values.tolist()

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def configure_kpis_for_api(self):
        if self.all_existing_kpis:
            if self.kpi_list or self.template_path:
                print 'all_existing_kpis is set to True => kpi list or kpi file data will be ignored'
            kpi_pks = self.kpi_static_data['pk'].values.tolist()
        else:
            kpi_pks = self.template_data['pk'].unique().tolist() if self.template_data is not None else self.kpi_list
            kpi_pks = kpi_pks if kpi_pks is not None else []
        kpi_pks = list(set(kpi_pks) - set(self.existing_configurations))
        kpi_pks = list(set(kpi_pks) - set(self.kpis_to_exclude))
        self.generate_insert_queries(kpi_pks)
        if self.insert_queries:
            merged_queries = self.merge_insert_queries()
            self.commit_to_db(merged_queries)
        if not self.insert_queries:
            print 'No kpis were added'

    def generate_insert_queries(self, kpi_pks):
        for pk in kpi_pks:
            attributes = self.create_attributes_dict(pk)
            query = insert(attributes, Consts.STATIC_KPI_VIEW_CONFIG)
            self.insert_queries.append(query)

    @staticmethod
    def create_attributes_dict(pk):
        attributes_dict = {'application': {0: 'API'},
                           'kpi_level_2_fk': {0: pk},
                           'kpi_level_1_fk': {0: 0},
                           'page': {0: ""}}
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
file_path=None, kpi_list=None, all_existing_kpis=False, kpis_to_exclude=None
file_path: optional attribute (if you want to enter kpi from xlsx file). Default value = None
kpi_list: optional attribute (if you want to enter kpi from the list). Default value = None
all_existing_kpis: optional attribute meaning that you want to add to api all kpis from static.kpi_level_2. 
                Default value: False 
kpis_to_exclude: optional attribute (if you want to prevent certain kpis from being added to the API). Default value = None

"""

if __name__ == '__main__':
    LoggerInitializer.init('test')
    Config.init()
    project_name = 'ccbza'
    # template_path = 'home/../filename.xlsx'
    # kpi_list = []
    # kpis_to_exclude = [999999]+range(2000, 2018)
    AddKPIsToAPI(project_name, all_existing_kpis=True).configure_kpis_for_api()
