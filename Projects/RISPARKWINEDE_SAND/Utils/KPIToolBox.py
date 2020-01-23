from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.RISPARKWINEDE_SAND.Utils.LocalConsts import Consts as LocalConst
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts


__author__ = 'nissand'

KPI_NEW_TABLE = 'report.kpi_level_2_results'


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


class RISPARKWINEDEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    TIME_DELTA = 90

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(
            self.project_name, DbUsers.CalculationEng)
        self.kpi_results_new_tables_queries = []
        # self.store_assortment = PSAssortmentDataProvider(self.data_provider).execute()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.current_date = datetime.now()
        self.assortment = Assortment(self.data_provider, self.output)
        self.store_assortment = self.assortment.store_assortment
        self.common = Common(self.data_provider)

    def main_calculation(self):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if not lvl3_result.empty:
            # wine assortment - wine shelves only
            self.wine_assortment_calculation(lvl3_result=lvl3_result)

            # existing assortment - whole session
            self.main_assortment_calculation(lvl3_result=lvl3_result)
        self.common.commit_results_data()

    # only SKU level assortment results for Discovery purposes
    def wine_assortment_calculation(self, lvl3_result):
        wine_kpi_fk = self.common.get_kpi_fk_by_kpi_type(LocalConst.WINE_AVAILABILITY)
        lvl3_result = lvl3_result[lvl3_result['kpi_fk_lvl2'] == wine_kpi_fk]
        if not lvl3_result.empty:
            for result in lvl3_result.itertuples():
                score = result.in_store * 100
                self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                               numerator_result=result.in_store, result=score,
                                               denominator_id=result.assortment_fk, denominator_result=1,
                                               score=score)

    def main_assortment_calculation(self, lvl3_result):
        """
        This function calculates the KPI results.
        """
        # lvl3_result = self.assortment.calculate_lvl3_assortment()
        wine_kpi_fk = self.common.get_kpi_fk_by_kpi_type(LocalConst.WINE_AVAILABILITY)
        lvl3_result = lvl3_result[~(lvl3_result['kpi_fk_lvl2'] == wine_kpi_fk)]
        if not lvl3_result.empty:
            for result in lvl3_result.itertuples():
                # start_new_date = datetime(2018, 2, 26) - timedelta(self.TIME_DELTA)
                start_new_date = self.visit_date - timedelta(self.TIME_DELTA)
                is_new = 0
                # ass_start_date = datetime(2017, 2, 9)
                ass_start_date = self.store_assortment[(self.store_assortment['product_fk'] == result.product_fk) &
                                                       (self.store_assortment['assortment_fk'] == result.assortment_fk) &
                                                       (self.store_assortment['assortment_group_fk'] ==
                                                        result.assortment_group_fk)]['start_date'].values[0]
                if np.datetime64(start_new_date) <= ass_start_date:
                    is_new = 1
                score = result.in_store * 100
                self.common.write_to_db_result(fk=result.kpi_fk_lvl3, numerator_id=result.product_fk,
                                               numerator_result=result.in_store, result=score,
                                               denominator_id=result.assortment_fk, denominator_result=1,
                                               score=score, score_after_actions=is_new)
                kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_SKU_KPI)
                parent_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_KPI)
                is_oos = 1
                if result.in_store:
                    is_oos = 0
                self.common.write_to_db_result(fk=kpi_fk, numerator_id=result.product_fk, numerator_result=is_oos,
                                               result=is_oos, denominator_id=parent_kpi_fk)
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if not pd.isnull(result.target) and not pd.isnull(result.group_target_date) and result.group_target_date <= self.visit_date:
                    denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res)) * 100
                if res >= 100:
                    score = 100
                else:
                    score = 0
                self.common.write_to_db_result(fk=result.kpi_fk_lvl2, numerator_id=result.assortment_fk,
                                               numerator_result=result.passes, result=res,
                                               denominator_id=result.assortment_group_fk,
                                               denominator_result=denominator_res, score=score)

            oos_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.OOS_KPI)

            dist_kpi_fk = self.common.get_kpi_fk_by_kpi_name(LocalConst.DIST)

            oos_numerator = len(lvl3_result[lvl3_result['in_store'] == 0])
            dist_numerator = len(lvl3_result[lvl3_result['in_store'] == 1])
            denominator = len(lvl3_result['in_store'])
            oos_res = np.divide(float(oos_numerator), float(denominator)) * 100
            dist_res = np.divide(float(dist_numerator), float(denominator)) * 100
            self.common.write_to_db_result(fk=oos_kpi_fk, numerator_id=oos_kpi_fk, numerator_result=oos_numerator,
                                           result=oos_res, denominator_result=denominator, score=oos_res)

            self.common.write_to_db_result(fk=dist_kpi_fk, numerator_id=dist_kpi_fk,
                                           numerator_result=dist_numerator, result=dist_res,
                                           denominator_result=denominator, score=dist_res)
