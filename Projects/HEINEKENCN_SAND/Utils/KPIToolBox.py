import os
from datetime import datetime

import pandas as pd
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

from Projects.HEINEKENCN_SAND.Utils.Fetcher import HEINEKENCNQueries
from Projects.HEINEKENCN_SAND.Utils.GeneralToolBox import HEINEKENCNGENERALToolBox
from Projects.HEINEKENCN_SAND.Utils.ParseTemplates import parse_template
from Projects.HEINEKENCN_SAND.Utils.ToolBox import HandleTemplate

__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
SHEET_NAME = 'Availability'
GROUP = 'KPI Name'
AVAILABILITY = 'Availability'
STORE_TYPE = 'Store Type'

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


class HEINEKENCNToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    PRODUCT = 'Product'
    SURVEY = 'Survey'
    BRAND= 'Brand'
    BRAND_CATEGORY = 'Brand,Category'

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
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
        self.tools = HEINEKENCNGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.tamplate_tools = HandleTemplate(self.data_provider, output, kpi_static_data=self.kpi_static_data)
        self.kpi_results_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.custom_templates = {}
        self.set_templates_data = {}
        self.scif_filters = {'Brand pk': 'brand_fk', 'Category pk': 'category_fk'}

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = HEINEKENCNQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name)
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpi_set_fk = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_name'] == AVAILABILITY]['kpi_set_fk'].iloc[0]
        self.set_templates_data[AVAILABILITY] = self.tamplate_tools.download_template(AVAILABILITY)
        products_template = pd.DataFrame(self.set_templates_data[AVAILABILITY])

        # If all 'store type' column in template is empty it will not be given as column in the json, therefore will be missing in the DataFrame
        if STORE_TYPE in products_template.columns:
            products_template = products_template.loc[products_template[STORE_TYPE].isin(['', self.store_type])]
        groups = products_template[GROUP].unique().tolist()
        for group in groups:
            group_template = products_template.loc[products_template[GROUP] == group]
            if group_template.empty:
                Log.info('No products were found for group \'{}\''.format(group))
            else:
                result = self.calculate_group(group_template)
                kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_fk'] == kpi_set_fk) &
                                              (self.kpi_static_data['kpi_name'] == group)]['kpi_fk'].iloc[0]
                atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_fk'] == kpi_set_fk) &
                                                     (self.kpi_static_data['atomic_kpi_name'] == group)]['atomic_kpi_fk'].iloc[0]
                self.write_to_db_result(atomic_fk, result, self.LEVEL3)
                self.write_to_db_result(kpi_fk, result, self.LEVEL2)
        self.write_to_db_result(kpi_set_fk, None, self.LEVEL1)

    def calculate_group(self, group_data):
        """
        Checks if a group is passed (having at least one product/ survey with correct answer)
        :param group_data: Not empty data Frame with a list of products ean code and surveys
        :return: 1 if group passed, 0 otherwise
        """
        product_counter = 0
        for i in xrange(len(group_data)):
            row = group_data.iloc[i]
            result = 0

            if row['Entity'] == self.PRODUCT:
                result = 1 if self.calculate_product(row['EAN Code']) else 0
            elif row['Entity'] == self.SURVEY:
                result = 1 if self.tools.check_survey_answer(('question_fk', int(float(row['Survey Question ID']))),
                                                             row['Survey Answer']) else 0
            elif self.BRAND in row['Entity'] or self.BRAND_CATEGORY in row['Entity']:
                result = 1 if self.calculate_brand(row) else 0

            product_counter += 1 if result else product_counter
        score = 100 if product_counter else 0
        return score

    def calculate_product(self, ean_code):
        filters = {'product_ean_code': ean_code}
        result = self.tools.calculate_availability(**filters)
        return result

    def calculate_brand(self, row):
        filters = self.get_filters_from_row(row, ['Brand pk', 'Category pk'])
        brand_sku = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        brand_sku = brand_sku['product_ean_code'].unique()
        return len(brand_sku)

    def get_filters_from_row(self, row, row_filters):
        """
        :param row: the row in template to get filters from
        :param row_filters: list of the filters needed (columns names in template)
        :return: a dictionary of filters in scif
        """
        filters = {}
        for current_filter in row_filters:
            if row[current_filter]:
                current_value = row[current_filter].split(",")
                try:
                    current_value = [float(x) for x in current_value]
                except TypeError:
                    current_value = current_value
                if current_filter in self.scif_filters:
                    filters[self.scif_filters[current_filter]] = current_value
                else:
                    filters[current_filter] = current_value
        return filters

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """

        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        score, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = HEINEKENCNQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
