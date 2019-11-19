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
    BINARY_FIELDS = ['session_relevance', 'scene_relevance', 'planogram_relevance',
                     'live_session_relevance', 'live_scene_relevance', 'is_percent']
    SALMON = '#FA8072'
    LIME = '#00FF00'
    BLUE = '#87CEFA'


class AddKPIs(object):
    def __init__(self, project_name, template_path=None, remove_duplicates=False, add_kpi_pks=False):
        self.project_name = project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.template_path = self.get_template_path(template_path)
        self.template_data = pd.read_excel(self.template_path)
        self.remove_duplicates = remove_duplicates
        self.kpi_counter = 0
        self.insert_queries = []
        self.output_path = self.get_output_file_path()
        self.error_cells = set()
        self.add_kpi_pks = add_kpi_pks

    @staticmethod
    def get_template_path(template_path):
        return template_path if template_path is not None else os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                            'new_tables_template.xlsx')

    def get_output_file_path(self):
        path_to_list = self.template_path.split('/')
        file_name = path_to_list[len(path_to_list)-1]
        output_path = os.path.join('/tmp', file_name)
        return output_path

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def add_kpis_from_template(self):
        self.validate_template()
        if len(self.error_cells) == 0:
            self.insert_into_kpi_lvl_2()
        else:
            self.highlight_errors_in_template()
            print 'errors found in template. see highlighted in path: {}'.format(self.output_path)

    def validate_template(self):
        self.check_similar_types()
        self.check_binary_fields()
        if not self.remove_duplicates:
            self.check_duplicate_in_template()

    def check_similar_types(self):
        kpi_types = set(self.template_data[Consts.KPI_TYPE].unique().tolist())
        existing_types = set(self.kpi_static_data[Consts.KPI_TYPE].unique().tolist())
        similar_types = kpi_types.intersection(existing_types)
        if similar_types:
            err_df = self.template_data[self.template_data[Consts.KPI_TYPE].isin(similar_types)]
            cells_list = [(i+1, Consts.KPI_TYPE, Consts.SALMON) for i in err_df.index.values]
            self.error_cells.update(cells_list)

    def check_binary_fields(self):
        binary_fields_df = self.template_data[Consts.BINARY_FIELDS]
        allowed_values = [1, 0, '1', '0', '1.0', '0.0', np.nan]
        for col in binary_fields_df.columns.tolist():
            err_df = binary_fields_df[~binary_fields_df[col].isin(allowed_values)]
            if len(err_df) > 0:
                cells_list = [(i+1, col, Consts.LIME) for i in err_df.index.values]
                self.error_cells.update(cells_list)

    def check_duplicate_in_template(self):
        template_data = self.template_data
        template_data['count'] = 1
        count_rows = template_data.groupby(Consts.KPI_TYPE, as_index=False).agg({'count': np.sum})
        count_rows = count_rows[count_rows['count'] != 1]
        if len(count_rows) > 0:
            duplicate_kpis = count_rows[Consts.KPI_TYPE].values.tolist()
            print 'duplicate kpis: ', str(duplicate_kpis)
            for kpi in duplicate_kpis:
                err_df = template_data[template_data[Consts.KPI_TYPE] == kpi]
                cells_list = [(i+1, Consts.KPI_TYPE, Consts.BLUE) for i in err_df.index.values]
                self.error_cells.update(cells_list)

    def highlight_errors_in_template(self):
        writer = pd.ExcelWriter(self.output_path, engine='xlsxwriter')
        self.template_data.to_excel(writer, sheet_name='Sheet1', index=False)

        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        # error_format = workbook.add_format({'fg_color': '#EEC93F'})
        for i, col, color in list(self.error_cells):
            value = self.template_data.loc[i-1, col]
            col_num = self.template_data.columns.get_loc(col)
            error_format = workbook.add_format({'fg_color': color})
            worksheet.write(i, col_num, value, error_format)
        writer.save()

    def insert_into_kpi_lvl_2(self):
        if self.remove_duplicates:
            self.template_data = self.template_data.drop_duplicates(subset=['type'], keep='first')
        for i, row in self.template_data.iterrows():
            attributes = self.create_attributes_dict(row)
            query = insert(attributes, Consts.STATIC_KPI_LVL_2)
            self.insert_queries.append(query)
        merged_queries = self.merge_insert_queries()
        # print merged_queries
        self.commit_to_db(merged_queries)

    def create_attributes_dict(self, kpi_row):
        attributes_dict = {'type': {0: kpi_row['type'].replace("'", "\\'").encode('utf-8')},
                           'client_name': {0: kpi_row['client_name'].replace("'", "\\'").encode('utf-8')},
                           'numerator_type_fk': {0: kpi_row['numerator_type_fk']},
                           'denominator_type_fk': {0: kpi_row['denominator_type_fk']},
                           'kpi_score_type_fk': {0: kpi_row['kpi_score_type_fk']},
                           'kpi_result_type_fk': {0: kpi_row['kpi_result_type_fk']},
                           'session_relevance': {0: kpi_row['session_relevance'] if not np.isnan(kpi_row['session_relevance']) else 0},
                           'scene_relevance': {0: kpi_row['scene_relevance'] if not np.isnan(kpi_row['scene_relevance']) else 0},
                           'planogram_relevance': {0: kpi_row['planogram_relevance'] if not np.isnan(kpi_row['planogram_relevance']) else 0},
                           'live_session_relevance': {0: kpi_row['live_session_relevance'] if not np.isnan(kpi_row['live_session_relevance']) else 0},
                           'live_scene_relevance': {0: kpi_row['live_scene_relevance'] if not np.isnan(kpi_row['live_scene_relevance']) else 0},
                           'is_percent': {0: kpi_row['is_percent'] if not np.isnan(kpi_row['is_percent']) else 0},
                           'kpi_target_type_fk': {0: kpi_row['kpi_target_type_fk']},
                           'kpi_calculation_stage_fk': {0: 3},
                           'valid_from': {0: '1990-01-01'},
                           'valid_until': {0: '2050-01-01'},
                           'initiated_by': {0: 'Custom'},
                           'context_type_fk': {0: kpi_row['context_type_fk']}}
        if self.add_kpi_pks:
            attributes_dict.update({'pk': {0: kpi_row['pk']}})
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
    project_name = 'batru-sand'
    template_path = '/home/natalyak/Desktop/batru/migration/kpis_to_db_template_new_kpi.xlsx'
    AddKPIs(project_name, template_path=template_path, add_kpi_pks=True).add_kpis_from_template()
