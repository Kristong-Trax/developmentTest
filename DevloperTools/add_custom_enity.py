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

__author__ = 'natalya'


class Consts(object):

    KPI_TYPE = 'type'
    KPI_CLIENT_NAME = 'client_name'
    STATIC_KPI_LVL_2 = 'static.kpi_level_2'
    STATIC_KPI_ENTITY_TYPE = 'static.kpi_entity_type'
    STATIC_CUSTOM_ENTITY = 'static.custom_entity'


class AddCustomEntities(object):
    def __init__(self, project_name, file_path):
        self.project_name = project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.entities_custom_entity = self.get_kpi_entity_type()
        self.existing_custom_entities = self.get_current_custom_entities()
        self.insert_queries = []
        self.template_path = file_path
        self.template_data = pd.read_excel(self.template_path)

    def get_kpi_entity_type(self):
        query = """ select * from static.kpi_entity_type where table_name='static.custom_entity' """
        entities_custom_entity = pd.read_sql_query(query, self.rds_conn.db)
        return entities_custom_entity

    def get_current_custom_entities(self):
        query = """ select * from static.custom_entity """
        custom_entities = pd.read_sql_query(query, self.rds_conn.db)
        return custom_entities

    def add_custom_entities_wo_parents(self, entity_types_to_upload):
        if self.entities_custom_entity.empty:
            print 'static.kpi_entity_type does not have entity types referring to table static.custom_entity. ' \
                  'No records will be added.'
            return
        for entity in entity_types_to_upload:
            entity_df = self.entities_custom_entity[self.entities_custom_entity['name'] == entity]
            entity_fk = entity_df['pk'].values[0] if len(entity_df) > 0 else None
            if entity_fk is None:
                print 'Entity {} does not exist in static.kpi_enyity_type. Proceeding to the next entity'.format(entity)
                continue
            values_to_add = self.get_custom_entity_values_to_add(entity)
            if len(values_to_add) == 0:
                print 'No new values will be added for entity_type: {}. Proceeding to the next entity'.format(entity)
            else:
                self.create_insert_queries(values_to_add, entity_fk)
        if len(self.insert_queries) == 0:
            print 'No records were added to db'
            return
        else:
            merged_queries = self.merge_insert_queries()
            self.commit_to_db(merged_queries)
            print '{} records were added to db'.format(len(self.insert_queries))

    def get_custom_entity_values_to_add(self, entity_type):
        if entity_type not in self.template_data.columns.values.tolist():
            print 'column corresponding to entity type {} does not exist in the template'.format(entity_type)
            return []
        template_values = self.template_data[~(self.template_data[entity_type].isnull())][entity_type].unique().tolist()
        existing_custom_entities = self.existing_custom_entities['name'].unique().tolist()
        values_to_add = list(set(template_values) - set(existing_custom_entities))
        return values_to_add

    def create_insert_queries(self, values_to_add, entity_fk):
        for value in values_to_add:
            attributes = self.create_attributes_dict(value, entity_fk)
            query = insert(attributes, Consts.STATIC_CUSTOM_ENTITY)
            self.insert_queries.append(query)

    @staticmethod
    def create_attributes_dict(value, entity_fk):
        attributes_dict = {'name': {0: value.replace("'", "\\'").encode('utf-8')},
                           'entity_type_fk': {0: entity_fk}}
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
entity_types: values to in static.kpi_entity_type for which the custom entity values are uploaded
File with values should have columns corresponding to the entity types that are being updated
"""

if __name__ == '__main__':
    LoggerInitializer.init('test')
    Config.init()
    project_name = 'pepsicouk'
    template_path = '/home/natalyak/Desktop/PepsicoUK/custom_entity_test.xlsx'
    entity_types = ['sub_brand', 'PDH_Format']
    AddCustomEntities(project_name, template_path).add_custom_entities_wo_parents(entity_types)
