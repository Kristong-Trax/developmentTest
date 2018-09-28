
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.DIAGEOBR.Utils.Fetcher import DIAGEOBRQueries
from Projects.DIAGEOBR.Utils.ToolBox import DIAGEOBRDIAGEOToolBox

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


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


class DIAGEOBRToolBox:

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.segment = self.get_business_unit_name()
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.get_match_display()
        self.set_templates_data = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.tools = DIAGEOBRDIAGEOToolBox(self.data_provider, output, kpi_static_data=self.kpi_static_data,
                                           match_display_in_scene=self.match_display_in_scene)
        self.kpi_results_queries = []

    def get_business_unit_name(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOBRQueries.get_business_unit_name(self.store_id)
        business_unit_name = pd.read_sql_query(query, self.rds_conn.db)
        if business_unit_name['business_unit_name'].empty:
            return ""
        else:
            return business_unit_name['business_unit_name'].values[0]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOBRQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = DIAGEOBRQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, set_name):
        """
        This function calculates the KPI results.
        """
        if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
            self.set_templates_data[set_name] = self.tools.download_template(set_name)

        # if set_name in ('MPA', 'New Products'):
        #     set_score = self.calculate_assortment_sets(set_name)
        # elif set_name in ('POSM',):
        #     set_score = self.calculate_posm_sets(set_name)
        if set_name == 'Visible to Customer':
            filters = {self.tools.VISIBILITY_PRODUCTS_FIELD: 'Y'}
            set_score = self.tools.calculate_visible_percentage(visible_filters=filters)
            self.save_level2_and_level3(set_name, set_name, set_score)
        elif set_name == 'Secondary Displays':
            set_score = self.tools.calculate_number_of_scenes(location_type='Secondary Shelf')
            self.save_level2_and_level3(set_name, set_name, set_score)
        else:
            return

        if set_score == 0:
            pass
        elif set_score is False:
            return

        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, set_score, self.LEVEL1)
        return

    def save_level2_and_level3(self, set_name, kpi_name, score):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name)]
        kpi_fk = kpi_data['kpi_fk'].values[0]
        atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
        self.write_to_db_result(kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(atomic_kpi_fk, score, self.LEVEL3)

    def calculate_posm_sets(self, set_name):
        """
        This function calculates every POSM-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            if self.store_channel is None:
                break

            kpi_res = self.tools.calculate_posm(display_name=params.get(self.tools.DISPLAY_NAME))
            score = 1 if kpi_res > 0 else 0
            if params.get(self.store_type) == self.tools.RELEVANT_FOR_STORE:
                scores.append(score)

            if score == 1 or params.get(self.store_type) == self.tools.RELEVANT_FOR_STORE:
                self.save_level2_and_level3(set_name, params.get(self.tools.DISPLAY_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def calculate_assortment_sets(self, set_name):
        """
        This function calculates every Assortment-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        segment = '{};{}'.format(self.store_type, self.segment)
        for params in self.set_templates_data[set_name]:
            if params.get(segment, '').capitalize() in (self.tools.RELEVANT_FOR_STORE,
                                                                self.tools.OR_OTHER_PRODUCTS):

                object_type = self.tools.ENTITY_TYPE_CONVERTER.get(params.get(self.tools.ENTITY_TYPE),
                                                                   'product_ean_code')
                objects = [str(params.get(self.tools.PRODUCT_EAN_CODE, params.get(self.tools.PRODUCT_EAN_CODE2, '')))]
                if params.get(self.store_type) == self.tools.OR_OTHER_PRODUCTS:
                    additional_objects = str(params.get(self.tools.ADDITIONAL_SKUS)).split(',')
                    objects.extend(additional_objects)
                filters = {object_type: objects}
                result = self.tools.calculate_assortment(**filters)
                score = 1 if result > 0 else 0
                scores.append(score)

                self.save_level2_and_level3(set_name, params.get(self.tools.PRODUCT_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def write_to_db_result(self, fk, score, level):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
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
        score = round(score, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = '%' if kpi_set_name in self.tools.KPI_SETS_WITH_PERCENT_AS_SCORE else ''
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, None, None)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = DIAGEOBRQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
