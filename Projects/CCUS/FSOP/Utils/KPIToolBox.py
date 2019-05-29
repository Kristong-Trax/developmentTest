from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.Programs.Utils.Fetcher import NEW_OBBOQueries
from Projects.CCUS.Programs.Utils.GeneralToolBox import NEW_OBBOGENERALToolBox
from Projects.CCUS.Programs.Utils.ParseTemplates import parse_template
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability as Availability_calc
from KPIUtils_v2.Calculations.SOSCalculations import SOS as SOS_calc
import numpy as np

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
STORE_TYPE_LIST = ['LS', 'CR', 'Drug', 'Value']
Availability = 'Availability'
SOS = 'SOS'
Survey = 'Survey'
Sheets = [Availability, SOS, Survey]
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Coke_FSOP_v1.xlsx')
CCNA = 'CCNA'


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


class FSOPToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output

        self.data_provider = data_provider
        self.commonV2 = CommonV2(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = NEW_OBBOGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        # self.rules = pd.read_excel(TEMPLATE_PATH).set_index('store_type').to_dict('index')
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.scene_data = self.load_scene_data()
        # self.kpi_set_fk = kpi_set_fk
        self.templates = {}
        self.parse_template()
        self.availability = Availability_calc(self.data_provider)
        self.SOS = SOS_calc(self.data_provider)



    def parse_template(self):
        for sheet in Sheets:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def calculate_availability(self):
        for i, row in  self.templates[Availability].iterrows():
            manufacturers =  self.sanitize_values(row['manufacturer'])
            brands = self.sanitize_values(row['Brand'])
            container = self.sanitize_values(row['Container'])
            attributte_4 = self.sanitize_values(row['att4'])
            required_brands = row['number_required_brands']
            required_sparkling = row['number_required_Sparkling']
            required_still= row['number_required_Still']
            filters = {'manufacturer_name': manufacturers, 'brand_name': brands, 'Container': container, 'att4': attributte_4 }

            for key in filters.keys():
                if type(filters[key]) is not list:
                    if pd.isna(filters[key]):
                        del filters[key]

            available_df = self.calculate_availability_df(**filters)

            if(required_brands):
                brands_available = available_df['brand_name'].unique().sum()

        pass


    def sanitize_values(self, item):
        if pd.isna(item):
            return item
        else:
            item = item.split(', ')
            return item

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = NEW_OBBOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        self.calculate_availability()


        if self.store_type in STORE_TYPE_LIST:
            result = 'Pass'
            score = self.determine_pass(self.rules[self.store_type])
            if not score:
                score2 = self.determine_pass(self.rules['All'], image_rule='>')
                if not score2:
                    result = 'Fail'
                else:
                    result = 'Exterior Only'

            self.write_to_db_result(3, name='Validation_KPI', result=result, score=score)
        return

    def determine_pass(self, rules, image_rule='<'):
        score = 1
        if sum([1 for t in self.templates['template_name'] if t in rules['template']]) < int(rules['target']):
            score = 0
        if int(self.scene_data.shape[0]) < int(rules['scene_count']):
            score = 0
        if eval('{} {} {}'.format(self.scene_data['Images'].sum(), image_rule, int(rules['image_count']))):
            score = 0
        return score

    def load_scene_data(self):
        query = '''
                select sc.scene_uid, (sc.number_of_probes - sc.number_of_ignored_probes) as 'Images'
                from probedata.session ses
                left join probedata.scene sc on ses.session_uid = sc.session_uid
                where sc.delete_time is null and sc.status = 6
                and ses.session_uid = '{}';
                '''.format(self.session_uid)
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)




    def calculate_availability_df(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            scif_mpis_diff = self.match_product_in_scene[['scene_fk', 'product_fk'] +
                                             list(self.match_product_in_scene.keys().difference(self.scif.keys()))]
            merged_df = pd.merge(self.scif[self.scif.facings != 0], scif_mpis_diff, how='outer',
                                 left_on=['scene_id', 'item_id'], right_on=['scene_fk', 'product_fk'])
            filtered_df = \
                merged_df[self.toolbox.get_filter_condition(merged_df, **filters)]
            # filtered_df = \
            #     self.match_product_in_scene[self.toolbox.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        if self.facings_field in filtered_df.columns:
            availability_df = filtered_df
        else:
            availability_df = filtered_df
        return availability_df

    def write_to_db_result(self, level, result=None, score=0, name=None):
        """
        This function creates the result Data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(score, level, result, name)
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

    def create_attributes_dict(self, score, level, result=None, name=None):
        """
        This function creates a Data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == self.kpi_set_fk]['kpi_set_name']\
                .values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), self.kpi_set_fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == self.kpi_set_fk]['kpi_set_name']\
                .values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 242, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        atomic_pks = tuple()
        if self.kpi_set_fk is not None:
            query = NEW_OBBOQueries.get_atomic_pk_to_delete(self.session_uid, self.kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = NEW_OBBOQueries.get_delete_session_results_query(self.session_uid, self.kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
