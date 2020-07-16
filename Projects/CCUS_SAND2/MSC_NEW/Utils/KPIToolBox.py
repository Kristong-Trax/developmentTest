from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.MSC_NEW.Utils.Fetcher import MSC_NEWQueries
from Projects.CCUS.MSC_NEW.Utils.GeneralToolBox import MSC_NEWGENERALToolBox
from Projects.CCUS.MSC_NEW.Utils.ParseTemplates import parse_template

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


class MSC_NEWToolBox:
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
        self.tools = MSC_NEWGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_data = parse_template(TEMPLATE_PATH, 'KPI')
        self.scif_filters = {'include manufacturer': 'manufacturer_name', 'exclude manufacturer': 'manufacturer_name',
                             'include att2': 'att2', 'exclude att2': 'att2',
                             'att4': 'att4', 'scene type': 'template_name'}
        self.row_filters = ['include manufacturer', 'exclude manufacturer', 'include att2', 'exclude att2', 'att4',
                            'scene type']

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MSC_NEWQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        results = {}
        template = self.kpi_data
        kpis = template['KPI Name'].unique().tolist()
        for kpi in kpis:
            results[kpi] = 0
        template_scene_types = template['scene type'].unique().tolist()
        scenes = self.scif['scene_fk'].unique().tolist()
        if scenes:
            for scene in scenes:
                bays = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]['bay_number']\
                                                                                        .unique().tolist()
                for bay in bays:
                    scene_data = self.scif.loc[self.scif['scene_fk'] == scene]
                    match_data = self.match_product_in_scene.loc[(self.match_product_in_scene['scene_fk'] == scene) &
                                                                 (self.match_product_in_scene['bay_number'] == bay)]
                    if scene_data['template_name'].values[0] in template_scene_types:
                        self.calculate_sos(match_data, results, kpis)
            for kpi in kpis:
                self.write_to_db_result(result=results[kpi], fk=self.kpi_static_data[(
                    self.kpi_static_data['atomic_kpi_name'] == str(kpi))]['atomic_kpi_fk'].values[0],
                                        score=0 if results[kpi] == 0 else 1, level=self.LEVEL3)
        return

    def calculate_sos(self, scene_data, results, kpis):
        """
            This function calculates the SOS for each cooler.
            returns dictionary with the number of passed collers for the session
            """
        total_filters = {}
        template = self.kpi_data
        for kpi in kpis:
            kpi_row = template.loc[template['KPI Name'] == kpi]
            filters = self.get_filters_from_row(kpi_row)
            filters['scene_fk'] = scene_data['scene_fk'].values[0]
            total_filters['scene_fk'] = scene_data['scene_fk'].values[0]
            result = self.tools.calculate_share_of_shelf(scene_data, sos_filters=filters, **total_filters)
            if result > 0.5:
                results[kpi] += 1
        return results

    def get_filters_from_row(self, kpi_row):
        """
        :param row: the row in template to get filters from
        :param row_filters: the filters needed (columns names in template)
        :return: a dictionary of filters in scif
        """
        filters = {}
        for current_filter in self.row_filters:
            if kpi_row[current_filter] is not u"" or None:
                if current_filter in self.scif_filters:
                    if 'exclude' in current_filter:
                        if kpi_row[current_filter].values[0]:
                            filters[self.scif_filters[current_filter]] = (kpi_row[current_filter].values[0].split(","),
                                                                          False)
                    else:
                        if kpi_row[current_filter].values[0]:
                            filters[self.scif_filters[current_filter]] = kpi_row[current_filter].values[0].split(",")
                else:
                    filters[current_filter] = kpi_row[current_filter]
        return filters

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

    def create_attributes_dict(self, fk, score, level, result=None, name=None):
        """
        This function creates a Data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 29]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
                                        ['atomic_kpi_name'].values[0],
                                        self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 243, result, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'result', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self, kpi_set_fk=None):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = MSC_NEWQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = MSC_NEWQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
