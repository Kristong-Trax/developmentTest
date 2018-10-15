# -*- coding: utf-8 -*-

import os

import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.NESTLEAPI_SAND.Utils.Fetcher import NESTLEAPIQueries
from Projects.NESTLEAPI_SAND.Utils.GeneralToolBox import NESTLEAPIGENERALToolBox
from Projects.NESTLEAPI_SAND.Utils.ParseTemplates import parse_template

__author__ = 'Ortal'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class NESTLEAPIToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.NESTLE = 'Nestlé'
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = NESTLEAPIGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.custom_templates = {}
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = NESTLEAPIQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name)
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def main_calculation(self, kpi_set_fk, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # if kpi_set_fk == 1:
        #     kpi_template = self.get_custom_template('Sheet1')
        #     if kpi_template.empty:
        #         return None
        #     for index, row in kpi_template.iterrows():
        #         pop_filter = {'product_ean_code' : row['product_ean_code']}
        #         category_filter = {'category': row['category']}
        #         num_of_products = self.tools.calculate_availability(**pop_filter)
        #         num_of_category = self.tools.calculate_availability(**category_filter)
        #         result = 0 if num_of_category == 0 else num_of_products / float(num_of_category)
        #         atomic_name = row['product_ean_code'] + '-' + row['product_name']
        #         self.write_to_db_result(self.LEVEL3, kpi_set_fk, atomic_name, result)
        #     return
        if kpi_set_fk == 2:
            categories = self.scif['category'].unique().tolist()
            for category in categories:
                filtered_category = self.scif[self.scif['category'] == category]
                filtered_manufacturer = filtered_category[
                                            filtered_category['manufacturer_name'].str.encode('utf-8') == self.NESTLE]
                for mode in ['facings', 'gross_len_add_stack']:
                    num_of_products_in_cat = sum(filtered_category[mode])
                    num_of_products_in_cat_and_manufacturer = sum(filtered_manufacturer[mode])
                    result = 0 if num_of_products_in_cat == 0 \
                        else num_of_products_in_cat_and_manufacturer / float(num_of_products_in_cat)
                    name = 'linear' if mode == 'gross_len_add_stack' else mode
                    atomic_name = 'sos for category_' + name + '_{}'.format(category)
                    self.write_to_db_result(self.LEVEL3, kpi_set_fk, atomic_name, result)
        elif kpi_set_fk == 3:
            brands = self.scif['brand_name'].unique().tolist()
            category_groups = self.scif['category_group_x'].unique().tolist()
            for category_group in category_groups:
                filtered_scif = self.scif[self.scif['category_group_x'] == category_group]
                for mode in ['facings', 'gross_len_add_stack']:
                    num_of_products_in_cat = sum(filtered_scif[mode])
                    for brand in brands:
                        brand_filter = filtered_scif.copy()
                        brand_filter = brand_filter[brand_filter['brand_name'] == brand]
                        num_of_products_in_brand = sum(brand_filter[mode])
                        result = 0 if num_of_products_in_cat == 0 else num_of_products_in_brand / float(num_of_products_in_cat)
                        name = 'linear' if mode == 'gross_len_add_stack' else mode
                        atomic_name = 'sos ' + name + ' for brand_{brand} and category_group_{category}'.format(
                            brand=brand, category=category_group)
                        self.write_to_db_result(self.LEVEL3, kpi_set_fk, atomic_name, result)
        elif kpi_set_fk == 4:
            try:
                shorted_df = self.scif[['product_ean_code', 'facings', 'facings_ign_stack', 'Customer_ean_code']]
            except:
                shorted_df = self.scif[['product_ean_code', 'facings', 'facings_ign_stack']]

            products_list = shorted_df['product_ean_code'].unique().tolist()
            for product_ean_code in products_list:
                filtered_df = shorted_df[shorted_df['product_ean_code'] == product_ean_code]
                num_of_facings = sum(filtered_df['facings'])
                # num_of_facings_ign_stack = sum(filtered_df['facings_ign_stack'])
                try:
                    ean_code = filtered_df.get('Customer_ean_code').dropna().values[0]
                except:
                    ean_code = product_ean_code
                display_name = 'SKU EAN Code: ' + ean_code
                self.write_to_db_result(self.LEVEL3, kpi_set_fk, result=num_of_facings, display_text=display_name, score=num_of_facings)
        return

    def write_to_db_result(self, level, kpi_set_fk, atomic_name=None, result=None, display_text=None, score=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(level, kpi_set_fk, result, atomic_name, display_text, score=score)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, level, kpi_set_fk, result=None, atomic_name=None, display_text=None, score=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        None, kpi_set_fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            if display_text:
                insert_into_display_text = display_text
            else:
                insert_into_display_text = atomic_name
            attributes = pd.DataFrame([(insert_into_display_text, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        result, kpi_set_fk, score)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'result', 'kpi_fk', 'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = NESTLEAPIQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
