
import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.CCUS_TEST.Utils.Fetcher import CCUSQueries
from Projects.CCUS_TEST.Utils.GeneralToolBox import CCUSGENERALToolBox
from Projects.CCUS_TEST.Utils.ParseComplexTemplates import parse_template

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Results.xlsx')


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


class CCUSConsts(object):

    # Temple Consts #
    SET_NAME = 'KPI Level 1 Name'
    KPI_NAME = 'KPI Level 2 Name'
    ATOMIC_NAME = 'KPI Level 3 Name'
    STORE_TYPE = 'Store_Type'
    TEMPLATE_GROUP = 'Template_Group'
    KPI_TYPE = 'KPI_Type'
    ENTITY = 'Entity'
    PARAM1 = 'Param1'
    PARAM2 = 'Param2'
    PARAM3 = 'Param3'
    PARAM4 = 'Param4'
    VALUE1 = 'Value1'
    VALUE2 = 'Value2'
    VALUE3 = 'Value3'
    VALUE4 = 'Value4'
    TARGET = 'Target'

    SEPARATOR = ','

    # Other #
    AVAILABILITY = 'Availability'
    ASSORTMENT = 'Assortment'
    SHARE_OF_SHELF = 'Share of Facing'


class CCUSToolBox(CCUSConsts):

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.get_missing_attributes(), on='product_fk', how='left', suffixes=['', '_1'])
        self.template_data = parse_template(TEMPLATE_PATH, 'Sheet1')
        self.tools = CCUSGENERALToolBox(self.data_provider, self.output, scif=self.scif)
        self.output_df = pd.read_excel(OUTPUT_PATH)
        self.kpi_static_data = self.get_kpi_static_data(set_name='CCUS Test')
        self.kpi_results_queries = []

    def get_missing_attributes(self):
        query = CCUSQueries.get_missing_attributes_data()
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_kpi_static_data(self, set_name):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCUSQueries.get_all_kpi_data(set_name)
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        output = []
        for p in xrange(len(self.template_data)):
            params = self.template_data.iloc[p]
            if params[self.STORE_TYPE] == self.store_type:
                filters = self.get_filters(params)
                if params[self.KPI_TYPE] == self.ASSORTMENT:
                    result = self.tools.calculate_assortment(assortment_entity=params[self.ENTITY], **filters)
                elif params[self.KPI_TYPE] == self.AVAILABILITY:
                    result = self.tools.calculate_availability(**filters)
                elif params[self.KPI_TYPE] == self.SHARE_OF_SHELF:
                    template_groups = filters.pop('template_group', None)
                    result = self.tools.calculate_share_of_shelf({'manufacturer_name': 'CCNA'},
                                                                 template_group=template_groups, **filters)
                else:
                    Log.warning("KPI of type '{}' is not supported".format(params[self.KPI_TYPE]))
                    continue
                threshold = params[self.TARGET] if params[self.TARGET] else None
                if threshold is not None:
                    score = 1 if result >= params[self.TARGET] else 0
                else:
                    score = None
                kpi_level = self.LEVEL3 if params[self.ATOMIC_NAME] else self.LEVEL2
                kpi_fk = self.get_kpi_fk(params, level=kpi_level)
                self.write_to_db_result(kpi_fk, (score, result, threshold), level=kpi_level)
                output.append(result)
            else:
                output.append('-')
        self.write_to_db_result(self.kpi_static_data['kpi_set_fk'].values[0], 100, level=self.LEVEL1)
        self.output_df[self.session_uid] = output
        writer = pd.ExcelWriter(OUTPUT_PATH, engine='xlsxwriter')
        self.output_df.to_excel(writer, index=False)

    def get_kpi_fk(self, params, level):
        kpi_name = params[self.KPI_NAME]
        if level == self.LEVEL3:
            atomic_name = params[self.ATOMIC_NAME]
            fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                      (self.kpi_static_data['kpi_name'] == kpi_name)]['atomic_kpi_fk'].values[0]
        elif level == self.LEVEL2:
            fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
        else:
            return None
        return fk

    def get_filters(self, params):
        filters = {}
        if params[self.PARAM1]:
            filters[params[self.PARAM1]] = params[self.VALUE1]
        if params[self.PARAM2]:
            filters[params[self.PARAM2]] = params[self.VALUE2]
        if params[self.PARAM3]:
            filters[params[self.PARAM3]] = params[self.VALUE3]
        if params[self.PARAM4]:
            filters[params[self.PARAM4]] = params[self.VALUE4]
        for param in filters.keys():
            value = filters[param]
            if value.startswith('~'):
                if '_id' in param or '_fk' in param:
                    filters[param] = (value[1:].split(self.SEPARATOR), self.tools.EXCLUDE_FILTER)
                else:
                    filters[param] = (value[1:].split(self.SEPARATOR), self.tools.NOT_CONTAIN_FILTER)
            else:
                filters[param] = value.split(self.SEPARATOR)
        if params[self.TEMPLATE_GROUP]:
            filters['template_group'] = params[self.TEMPLATE_GROUP].split(self.SEPARATOR)
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
            score, score_2, score_3 = score
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, score_2, score_3)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name',
                                               'score', 'score_2', 'score_3'])
        elif level == self.LEVEL3:
            score, result, threshold = score
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, threshold, result)],
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
        delete_queries = CCUSQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
