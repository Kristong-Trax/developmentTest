from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.GOLD_PEAK_BLOCK.Utils.Fetcher import GOLD_PEAK_BLOCKQueries
from Projects.CCUS.GOLD_PEAK_BLOCK.Utils.GeneralToolBox import GOLD_PEAK_BLOCKGeneralToolBox
from Projects.CCUS.GOLD_PEAK_BLOCK.Utils.ParseTemplates import parse_template

__author__ = 'Shani'

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


class GOLD_PEAK_BLOCKToolBox:
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
        self.tools = GOLD_PEAK_BLOCKGeneralToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_data = parse_template(TEMPLATE_PATH, 'KPIs')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = GOLD_PEAK_BLOCKQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_top_shelf(self.kpi_data.loc[self.kpi_data['kpi type'] =='calculate_top_shelf'])
        self.calculate_top_shelf_sos(self.kpi_data.loc[self.kpi_data['kpi type'] =='calculate_top_shelf_sos'])
        self.calculate_gold_peak_tea_block(self.kpi_data.loc[self.kpi_data['kpi type'] =='calculate_gold_peak_tea_block'])

        return

    def calculate_top_shelf(self, relevant_kpi_data):
        score = 1
        result = 0
        template_group=str(relevant_kpi_data['Template group'].values[0])
        relevant_scenes=self.scif.loc[self.scif['template_group']==template_group]
        scenes = relevant_scenes['scene_fk'].unique().tolist()
        if scenes:
            for scene in scenes:
                if self.scif[self.scif['scene_fk'] == scene]['template_group'].values[0] == \
                        relevant_kpi_data['Template group'].values[0]:
                    scene_data = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene].merge(self.all_products, on=['product_fk'])
                    if not scene_data.loc[(scene_data['brand_name'] == relevant_kpi_data['Brand'].values[0]) & (scene_data['shelf_number']==1)].empty:
                        score=0
                        result=len(scene_data.loc[(scene_data['brand_name'] == relevant_kpi_data['Brand'].values[0]) & (scene_data['shelf_number']==1)])
                        self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                                result=result,
                                                score=score, level=self.LEVEL3)
                        return
            self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                            result=result,
                                            score=score, level=self.LEVEL3)
            return
        return

    def calculate_top_shelf_sos(self, relevant_kpi_data):
        score = 1
        result = 0
        facings_on_top_shelf=0
        total_facings_on_shelfs=0
        template_group = str(relevant_kpi_data['Template group'].values[0])
        relevant_scenes = self.scif.loc[self.scif['template_group'] == template_group]
        scenes = relevant_scenes['scene_fk'].unique().tolist()
        if scenes:
            for scene in scenes:
                if self.scif[self.scif['scene_fk'] == scene]['template_group'].values[0] == \
                        relevant_kpi_data['Template group'].values[0]:
                    scene_data = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene].merge(
                        self.all_products, on=['product_fk'])
                    numerator_scene_data= scene_data.loc[(scene_data['brand_name'] == relevant_kpi_data['Brand'].values[0]) & (scene_data[
                        'shelf_number'] == 1)]
                    denominator_scene_data=scene_data.loc[scene_data['brand_name'] == relevant_kpi_data['Brand'].values[0]]
                    if not numerator_scene_data.empty:
                        facings_on_top_shelf=+len(numerator_scene_data)
                    if not denominator_scene_data.empty:
                        total_facings_on_shelfs=+len(denominator_scene_data)
            if total_facings_on_shelfs:
                result=float(facings_on_top_shelf)/total_facings_on_shelfs
                if result==0:
                    score=1
                else:
                    score=0
                self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                            result=result,
                                            score=score, level=self.LEVEL3)
                return
            else:
                result='N/A'
                score=1
                self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                            result=result,
                                            score=score, level=self.LEVEL3)
        return

    def calculate_gold_peak_tea_block(self, relevant_kpi_data):
        template_group = str(relevant_kpi_data['Template group'].values[0])
        relevant_scenes = self.scif.loc[self.scif['template_group'] == template_group]
        scenes = relevant_scenes['scene_fk'].unique().tolist()
        if scenes:
            filters={'brand_name':relevant_kpi_data['Brand'].values[0], 'template_group': relevant_kpi_data['Template group'].values[0]}
            result=self.tools.calculate_block_together(minimum_block_ratio=0.01, vertical=True, horizontal=True, **filters)
            if result:
                score=1
                self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                                result=result,
                                                score=score, level=self.LEVEL3)
            else:
                score=0
                self.write_to_db_result(name=self.kpi_static_data.loc[self.kpi_static_data['atomic_kpi_name'] == relevant_kpi_data['KPI Name'].values[0]]['atomic_kpi_name'].values[0],
                                                result=result,
                                                score=score, level=self.LEVEL3)
        return




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
        if level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 30]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 244, result, self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == name]['atomic_kpi_fk'].values[0])],
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
            query = GOLD_PEAK_BLOCKQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = GOLD_PEAK_BLOCKQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
