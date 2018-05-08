
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.GILLETTEUS_SAND.Utils.Fetcher import GILLETTEUS_SANDQueries
from Projects.GILLETTEUS_SAND.Utils.GeneralToolBox import GILLETTEUS_SANDGENERALToolBox

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

MAX_PARAMS = 2
SEPARATOR = ','
BINARY = 'binary'
PERCENTAGE = 'percentage'


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


class GILLETTEUS_SANDTemplateConsts(object):

    KPI_SET_NAME = 'KPI Level 1 Name'
    KPI_NAME = 'KPI Level 2 Name'
    ATOMIC_KPI_NAME = 'KPI Level 3 Name'
    KPI_TYPE = 'KPI_Type'
    ATOMIC_KPI_TYPE = 'Atomic_KPI_Type'
    STORE_TYPE = 'Store_Type'
    PARAMS = 'Param'
    VALUES = 'Value'
    KPI_TARGET = 'Target Level 2'
    ATOMIC_KPI_TARGET = 'Target Level 3'
    KPI_SCORE = 'Score'


class GILLETTEUS_SANDToolBox(GILLETTEUS_SANDTemplateConsts):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    STORE_TYPE = 'SUPERMARKET'

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
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.match_display_in_scene = self.get_match_display()
        self.general_tools = GILLETTEUS_SANDGENERALToolBox(self.data_provider, output)
        self.template_data = self.general_tools.get_json_data(TEMPLATE_PATH)
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = GILLETTEUS_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = GILLETTEUS_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculations(self):
        """
        This function calculates every Atomic KPI separately and saves it in a score dictionary,
        later to be used by the 'calculate_kpi' function.
        """
        for params in self.template_data:
            atomic_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == params.get(self.KPI_SET_NAME)) &
                                               (self.kpi_static_data['kpi_name'] == params.get(self.KPI_NAME)) &
                                               (self.kpi_static_data['atomic_kpi_name'] == params.get(self.ATOMIC_KPI_NAME))]
            if atomic_data.empty:
                Log.warning('Atomic KPI {} doesn\'t exist'.format(params.get(self.ATOMIC_KPI_NAME)))
                continue
            kpi_type = params.get(self.ATOMIC_KPI_TYPE)
            filters = self.get_filters(params)
            if kpi_type == 'Blocked Together':
                result = self.general_tools.calculate_block_together(include_empty=self.general_tools.EXCLUDE_EMPTY,
                                                                     **filters)
                if result is None:
                    score = None
                else:
                    score = 1 if result else 0
            elif kpi_type == 'Count of Display_Entity':
                score = self.calculate_share_of_display(filters)
            else:
                Log.warning('KPI type {} is not valid'.format(kpi_type))
                continue

            if score is not None:
                atomic_fk = atomic_data['atomic_kpi_fk'].values[0]
                kpi_fk = atomic_data['kpi_fk'].values[0]
                set_fk = atomic_data['kpi_set_fk'].values[0]

                self.write_to_db_result(atomic_fk, score, self.LEVEL3)
                if isinstance(score, (list, tuple)):
                    result, threshold, score = score
                if params.get(self.KPI_SCORE) == BINARY:
                    score *= 100
                self.write_to_db_result(kpi_fk, score, self.LEVEL2)
                self.write_to_db_result(set_fk, score, self.LEVEL1)

    def calculate_share_of_display(self, filters):
        """
        This function calculates Share-of-Display typed Atomics
        """
        if 'template_name' in filters.keys():
            scenes = self.scif[self.scif['template_name'].str.contains(filters['template_name'][0])]['scene_id'].unique().tolist()
        elif 'location_type' in filters.keys():
            scenes = self.scif[self.scif['location_type'].str.contains(filters['location_type'][0])]['scene_id'].unique().tolist()
        else:
            scenes = self.scif['scene_id'].unique().tolist()
        if scenes:
            filtered_matches = self.match_display_in_scene[self.match_display_in_scene['scene_id'].isin(scenes)]
            if not filtered_matches.empty:
                gillette_displays = filter(lambda x: x.strip() in filters.get('display_name'), filtered_matches['display_name'].tolist())
                share_of_display = (len(gillette_displays) / float(len(filtered_matches))) * 100
                return len(gillette_displays), len(filtered_matches), round(share_of_display, 2)
            else:
                return None
        else:
            return None

    def get_filters(self, params):
        """
        This function extracts the filter parameters from the static template, and returns them as list of dictionaries.
        """
        filters = {}
        for i in xrange(1, MAX_PARAMS + 1):
            field = params.get('{}{}'.format(self.PARAMS, i))
            if field:
                values = params.get('{}{}'.format(self.VALUES, i))
                filters[field] = values.split(SEPARATOR)
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
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            if isinstance(score, tuple):
                result, threshold, score = score
            else:
                result = threshold = None
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, result, threshold)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'result',
                                               'threshold'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = GILLETTEUS_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
