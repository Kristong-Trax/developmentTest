from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.XM.Utils.Fetcher import XMQueries
from Projects.CCUS.XM.Utils.GeneralToolBox import XMGENERALToolBox
from Projects.CCUS.XM.Utils.ParseTemplates import parse_template

__author__ = 'Uri'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_GROUP = ['Programs', 'Display']
ATT_5_LIST = ['National-Meals', 'Customer-Meals', 'Local-Meals', 'No POP', 'National-Beauty',
          'Local-Beauty', 'Customer-Beauty', 'National-Wellness','Local-Wellness', 'Customer-Wellness',
          'National-Sports', 'Local-Sports', 'Customer-Sports', 'National-Refreshment', 'Local-Refreshment',
          'Customer-Refreshment', 'National-HomeMediaLeisure', 'Cutomer-HomeMediaLeisure', 'Local-HomeMediaLeisure',
          'All Other-Hydration']
STORE_TYPE_LIST=['LS','CR','Drug','Value']
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'TemplateV4.xlsx')


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


class XMToolBox:
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = XMGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.xm_data = parse_template(TEMPLATE_PATH, 'main')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = XMQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if str(self.visit_date)>='2018-01-14' and str(self.visit_date)<='2018-02-15':
            scenes = self.scif['scene_id'].unique().tolist()
            if scenes:
                for kpi in self.xm_data['KPI Name'].unique().tolist():
                    filters = {}
                    kpi_data = self.xm_data[self.xm_data['KPI Name'] == kpi]
                    filters['template_group'] = kpi_data['Template group']
                    filters['template_name'] = kpi_data['Template name']
                    scene_data = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
                    poc_data = self.get_point_of_contacts(scene_data)
            return
        return

    def get_point_of_contacts(self, scene_data):
        pass

    def write_to_db_result(self, score, level, result=None, name=None):
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
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 32]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), 32)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 32]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 246, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self, kpi_set_fk=None):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn.disconnect_rds()
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = XMQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = XMQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()


