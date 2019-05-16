import os

import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.GOOGLEUS_SAND.Utils.Fetcher import GOOGLEUS_SANDQueries
from Projects.GOOGLEUS_SAND.Utils.GeneralToolBox import GOOGLEUS_SANDGENERALToolBox
from Projects.GOOGLEUS_SAND.Utils.ParseTemplates import parse_template

__author__ = 'Ortal'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
YES = 'Y'


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


class GOOGLEUS_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.tools = GOOGLEUS_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.price_data = self.get_price_data()
        self.kpi_results_queries = []
        self.custom_templates = {}

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = GOOGLEUS_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_price_data(self):
        """
        This function extracts the products' prices data from probedata.match_product_in_probe_details.
        """
        query = GOOGLEUS_SANDQueries.get_prices(self.session_uid)
        prices = pd.read_sql_query(query, self.rds_conn.db)
        prices = prices.merge(self.all_products[['product_fk', 'product_ean_code', 'product_name']],
                              on='product_fk', how='left')
        return prices.fillna('')

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name)
            if 'Unnamed: 0' in template.columns:
                template = parse_template(TEMPLATE_PATH, name, 1)
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = GOOGLEUS_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, set_fk):
        """
        This function calculates the KPI results.
        """
        kpi_template = self.get_custom_template('KPIs')
        set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == set_fk]['kpi_set_name'].iloc[0]
        kpi_template = kpi_template.loc[kpi_template['KPI Group'] == set_name]
        sum_kpi_score = 0
        for index, row in kpi_template.iterrows():
            total_kpi_score = 0
            kpi_name = row['KPI Display Name']
            kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                            (self.kpi_static_data['kpi_name'] == kpi_name)].iloc[0]
            if row['Availability'] == YES:
                relevant_tab = 'Availability'
                atomic_scores = self.calculate_availability(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Count of Display'] == YES:
                relevant_tab = 'Count of Display'
                atomic_scores = self.count_of_display(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Survey'] == YES:
                relevant_tab = 'Survey'
                atomic_scores = self.check_survey_answer(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Price Tag'] == YES:
                relevant_tab = 'Price Tag'
                atomic_scores = self.check_price_tag(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Price Compliance'] == YES:
                relevant_tab = 'Price Compliance'
                atomic_scores = self.calculate_price_compliance(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Count of Scene'] == YES:
                relevant_tab = 'Count of Scene'
                atomic_scores = self.count_of_scene(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            if row['Adjacency'] == YES:
                relevant_tab = 'Adjacency'
                atomic_scores = self.calculate_adjacency(kpi_name, relevant_tab, set_name)
                total_kpi_score += atomic_scores

            self.write_to_db_result(kpi_data['kpi_fk'], total_kpi_score * 100, self.LEVEL2)
            sum_kpi_score += total_kpi_score

        return sum_kpi_score * 100

    def calculate_availability(self, kpi_name, tab, set_name):
        atomic_scores = 0
        availability_template = self.get_custom_template(tab)
        availability_template = availability_template.loc[(availability_template['KPI Set'].str.encode('utf-8') ==
                                                           set_name.encode('utf-8'))]
        availability_template = availability_template.loc[(availability_template['KPI Display Name'].str.encode('utf-8')
                                                           == kpi_name.encode('utf-8'))]
        if not pd.isnull(availability_template['Template_name'].iloc[0]):
            for index, row in availability_template.iterrows():
                atomic_name = row['Atomic KPI Name']
                filters = {}
                if not pd.isnull(row['Product_name']):
                    filters['product_name'] = row['Product_name'].split(', ')
                if not pd.isnull(row['Template_name']):
                    filters['template_name'] = row['Template_name']
                score = self.tools.calculate_availability(**filters)
                atomic_score_for_kpi = 1 * float(row['WEIGHT']) if score >= int(row['TARGET']) else 0
                atomic_score = 1 if score >= 1 else 0
                atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                                 (self.kpi_static_data['kpi_set_name'] == set_name) &
                                                 (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
                self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100, level=self.LEVEL3)
                atomic_scores += atomic_score_for_kpi
            return atomic_scores

    def count_of_display(self, kpi_name, tab, set_name):
        atomic_scores = 0
        displays_template = self.get_custom_template(tab)
        displays_template = displays_template.loc[(displays_template['KPI Set'] == set_name)]
        displays_template = displays_template.loc[(displays_template['KPI Display Name'] == kpi_name)]
        for index, row in displays_template.iterrows():
            atomic_name = row['Atomic KPI Name']
            displays = self.match_display_in_scene.loc[(self.match_display_in_scene['display_name'] == row['Display Name'])]
            score = len(displays)
            atomic_score_for_kpi = 1 * float(row['WEIGHT']) if score >= int(float(row['TARGET'])) else 0
            atomic_score = 1 if score >= 1 else 0
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                             (self.kpi_static_data['kpi_set_name'] == set_name) &
                                             (self.kpi_static_data['kpi_name'] == kpi_name)][
                'atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100, level=self.LEVEL3)
            atomic_scores += atomic_score_for_kpi
        return atomic_scores

    def check_survey_answer(self, kpi_name, tab, set_name):
        """
        This function calculates 'Survey Question' typed KPI, and saves the results to the DB.
        """
        atomic_scores = 0
        survey_template = self.get_custom_template(tab)
        survey_template = survey_template.loc[(survey_template['KPI Set'] == set_name)]
        survey_template = survey_template.loc[(survey_template['KPI Display Name'] == kpi_name)]
        for index, row in survey_template.iterrows():
            target_answer = row['Target']
            atomic_name = row['Atomic KPI Name']
            survey_answer = self.tools.get_survey_answer(row['Survey_Question_id'], row['Group_Name'])
            survey_answer = '-' if survey_answer is None else survey_answer
            atomic_score_for_kpi = 1 * float(row['WEIGHT']) if survey_answer == target_answer else 0
            atomic_score = 1 if survey_answer == target_answer >= 1 else 0
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                             (self.kpi_static_data['kpi_set_name'] == set_name) &
                                             (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100, level=self.LEVEL3)
            atomic_scores += atomic_score_for_kpi
        return atomic_scores

    def check_price_tag(self, kpi_name, tab, set_name):
        """
        This function calculates 'Survey Question' typed KPI, and saves the results to the DB.
        """
        atomic_scores = 0
        price_tag_template = self.get_custom_template(tab)
        price_tag_template = price_tag_template.loc[(price_tag_template['KPI Set'] == set_name)]
        price_tag_template = price_tag_template.loc[(price_tag_template['KPI Display Name'] == kpi_name)]
        for index, row in price_tag_template.iterrows():
            atomic_name = row['Atomic KPI Name']
            product_data = self.price_data[self.price_data['product_name'].isin(row['product_name'].split(', '))]
            if not product_data.empty:
                product_data = product_data.iloc[0]
                atomic_score_for_kpi = 1 * float(row['WEIGHT']) if product_data['price'] != '' else 0
                atomic_score = 1 if product_data['price'] != '' else 0
                atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                                 (self.kpi_static_data['kpi_set_name'] == set_name) &
                                                 (self.kpi_static_data['kpi_name'] == kpi_name)][
                    'atomic_kpi_fk'].values[0]
                self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100,
                                        level=self.LEVEL3)
                atomic_scores += atomic_score_for_kpi
        return atomic_scores

    def count_of_scene(self, kpi_name, tab, set_name):
        atomic_scores = 0
        price_tag_template = self.get_custom_template(tab)
        price_tag_template = price_tag_template.loc[(price_tag_template['KPI Set'] == set_name)]
        price_tag_template = price_tag_template.loc[(price_tag_template['KPI Display Name'] == kpi_name)]
        for index, row in price_tag_template.iterrows():
            atomic_name = row['Atomic KPI Name']
            filters = {'template_group': row['Template Group']}
            result = self.tools.calculate_number_of_scenes(**filters)
            atomic_score_for_kpi = 1 * float(row['WEIGHT']) if result >= 1 else 0
            atomic_score = 1 if result >= 1 else 0
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                             (self.kpi_static_data['kpi_set_name'] == set_name) &
                                             (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100, level=self.LEVEL3)
            atomic_scores += atomic_score_for_kpi
        return atomic_scores

    def calculate_price_compliance(self, kpi_name, tab, set_name):
        """
        This function calculates 'Price Compliance' typed KPI, and saves the results to the DB.
        """
        atomic_scores = 0
        price_compliance_template = self.get_custom_template(tab)
        price_compliance_template = price_compliance_template.loc[(price_compliance_template['KPI Set'] == set_name)]
        price_compliance_template = price_compliance_template.loc[(price_compliance_template['KPI Display Name'] == kpi_name)]
        for index, row in price_compliance_template.iterrows():
            atomic_name = row['Atomic KPI Name']
            product_data = self.price_data[self.price_data['product_name'].isin(row['product_name'].split(', '))]
            if not product_data.empty:
                product_data = product_data.iloc[0]
                atomic_score_for_kpi = 1 * float(row['WEIGHT']) if product_data['price'] == float(row['TARGET']) else 0
                atomic_score = 1 if product_data['price'] == float(row['TARGET']) else 0
                atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                                 (self.kpi_static_data['kpi_set_name'] == set_name) &
                                                 (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
                self.write_to_db_result(atomic_fk, score=atomic_score, result=atomic_score_for_kpi * 100,
                                        level=self.LEVEL3)
                atomic_scores += atomic_score_for_kpi
        return atomic_scores

    def calculate_adjacency(self, kpi_name, tab, set_name):
        """
        This function calculates 'Price Complaince' typed KPI, and saves the results to the DB.
        """
        atomic_scores = 0
        adjacency_template = self.get_custom_template(tab)
        adjacency_template = adjacency_template.loc[(adjacency_template['KPI Set'] == set_name)]
        adjacency_template = adjacency_template.loc[(adjacency_template['KPI Display Name'] == kpi_name)]
        for index, row in adjacency_template.iterrows():
            result = None
            atomic_name = row['Atomic KPI Name']
            product_name = row['product_name']
            scene_data = self.scif[(self.scif['template_name'] == row['Template_name']) &
                                   (self.scif['product_name'] == product_name)]
            if not scene_data.empty:
                scene = scene_data['scene_id'].iloc[0]
                entity = 'product_name'
                matrix = self.tools.position_graphs.get_entity_matrix(scene, entity)
                for mrow in matrix:
                    for i in xrange(0, len(mrow) - 1):
                        if mrow[i] == product_name:
                            if row['side'] == 'Right':
                                if mrow[i+1]:
                                    result = mrow[i+1]
                                    break
                            elif row['side'] == 'Left':
                                if mrow[i-1]:
                                    result = mrow[i-1]
                                    break
                    if result:
                        break
            if result is None:
                result = '-'
            atomic_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                             (self.kpi_static_data['kpi_set_name'] == set_name) &
                                             (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_fk, score=0, result=result, level=self.LEVEL3)
        return atomic_scores

    def write_to_db_result(self, fk, score, level, result=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level, result)
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

    def create_attributes_dict(self, fk, score, level, result=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            if isinstance(result, float):
                result = round(result, 2)
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, result, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = GOOGLEUS_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
