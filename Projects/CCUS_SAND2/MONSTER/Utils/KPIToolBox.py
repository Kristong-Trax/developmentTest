from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.MONSTER.Utils.Fetcher import MONSTERQueries
from Projects.CCUS.MONSTER.Utils.GeneralToolBox import MONSTERGENERALToolBox

__author__ = 'Ortal'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_COLD = [19, 116]
MUTANT_PRODUCTS = ['7084702652', '7084702650', '355', '356']
SOS_TARGET = 0.12
SSD = 'SSD'
ATT2 = ['Diet Coke', 'Coke Classic', 'Coke Zero', 'Coke Life', 'FANTA', 'Mello Yello', 'Sprite']
MOUNTAIN_DEW = ['149', '150', '193', '194', '234', '235', '300', '301', '3196', '3197', '3198', '3199', '3195',
                '1245', '5044', '15036', '3041']

SOS_TARGET_KPI1 = 0.50
SOS_TARGET_KPI2 = 0.12
SSD = 'SSD'
ATT2 = ['Diet Coke', 'Coke Classic', 'Coke Zero', 'Coke Life', 'FANTA', 'Mello Yello', 'Sprite']
MOUNTAIN_DEW = ['149', '150', '193', '194', '234', '235', '300', '301', '3196', '3197', '3198', '3199', '3195',
                '1245', '5044', '15036', '3041']


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


class MONSTERToolBox:
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
        self.tools = MONSTERGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.ssd_score = 0   # Save the current score for Kpi1, fail Kp12 in case Kpi1 succeed.

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MONSTERQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        score = 0
        kpi1 = self.calculate_kpi1()
        kpi2 = self.calculate_kpi2()
        if kpi1 and kpi2:
            score = 1
            self.write_to_db_result(result=score,
                                    score=score, level=self.LEVEL1)
            return True
        else:
            self.write_to_db_result(result=score,
                                    score=score, level=self.LEVEL1)
            return False

    def calculate_availability(self, filters1, filters2, check_sos=False, type=None):
        score = 0
        scif_scenes = self.scif.loc[self.scif['template_fk'].isin(TEMPLATE_COLD)]
        scenes = scif_scenes['scene_fk'].unique().tolist()
        bays = self.match_product_in_scene['bay_number'].unique().tolist()
        if scenes:
            for scene in scenes:
                for bay in bays:
                    filters1['scene_fk'] = scene
                    filters1['bay_number'] = bay
                    filters2['scene_fk'] = scene
                    filters2['bay_number'] = bay
                    if check_sos:
                        numerator_result = self.tools.calculate_availability(front_facing='Y', **filters2)
                        denominator_filters = {'scene_fk': scene, 'bay_number': bay}
                        denominator_result = self.tools.calculate_availability(front_facing='Y', **denominator_filters)
                        result = 0 if denominator_result == 0 else (numerator_result / float(denominator_result))
                        if type == "calc1":
                            target = SOS_TARGET_KPI1
                        elif type == "calc2":
                            target = SOS_TARGET_KPI2
                        if result < target:
                            continue
                    result1 = self.tools.calculate_availability(**filters1)
                    result2 = self.tools.calculate_availability(**filters2)
                    score = 1 if (result1 and result2) >= 1 else 0
                    if score > 0:
                        return score
        return score

    def calculate_kpi1(self):
        filters1 = {'product_ean_code': ['355', '356', '7084702650', '7084702652']}
        filters2 = {'att2': ATT2, 'att4': SSD, 'manufacturer_fk': 1}
        score = self.calculate_availability(filters1, filters2, check_sos=True,type="calc1")
        if score > 0:
            self.write_to_db_result(name='Is Mutant in the same door as Coke SSD', result=score,
                                    score=1, level=self.LEVEL3)
            self.ssd_score = 1
            return True
        else:
            self.write_to_db_result(name='Is Mutant in the same door as Coke SSD?', result=score,
                                    score=0, level=self.LEVEL3)
            self.ssd_score = 0
            return False

    def calculate_kpi2(self):
        result = 0
        filters1 = {'product_ean_code': ['355', '356', '7084702650', '7084702652']}
        filters2 = {'product_ean_code': MOUNTAIN_DEW}
        score = self.calculate_availability(filters1, filters2, check_sos=True,type="calc2")
        if score > 0 and self.ssd_score == 0:
            self.write_to_db_result(name='Is Mutant in the same door as Mountain Dew?', result=score,
                                    score=1, level=self.LEVEL3)
            return True
        else:
            self.write_to_db_result(name='Is Mutant in the same door as Mountain Dew?', result=result,
                                    score=0, level=self.LEVEL3)
            return False

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
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 27]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), 27)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 27]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 241, result)],
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = MONSTERQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = MONSTERQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
