
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import numpy as np
import pandas as pd
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class HEINEKENTWToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    DIST_STORE_LVL1 = 1014
    OOS_STORE_LVL1 = 1015
    DIST_STORE_LVL2 = 1016
    OOS_STORE_LVL2 = 1017

    DIST_CATEGORY_LVL1 = 1018
    OOS_CATEGORY_LVL1 = 1019
    DIST_CATEGORY_LVL2 = 1020
    OOS_CATEGORY_LVL2 = 1021

    DISTRIBUTION= 4
    OOS=5

    MANUFACTURER_FK = 175  # heinken manfucturer

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
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
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_queries = []
        self.assortment = Assortment(self.data_provider, self.output)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        self.category_assortment_calculation(lvl3_result)
        self.store_assortment_calculation(lvl3_result)
        self.common.commit_results_data_to_new_tables()

        # self.common.commit_results_data_to_new_tables()

    def category_assortment_calculation(self, lvl3_result):
        """
        This function calculates 3 levels of assortment :
        level3 is assortment SKU
        level2 is assortment groups
        """
        if not lvl3_result.empty:
            # cat_df = self.scif[['product_fk', 'category_fk']]
            cat_df = self.all_products[['product_fk','category_fk']]
            lvl3_with_cat = lvl3_result.merge(cat_df, on='product_fk', how='left')
            lvl3_with_cat = lvl3_with_cat[lvl3_with_cat['category_fk'].notnull()]

            for result in lvl3_with_cat.itertuples():
                if result.in_store == 1:
                    score = self.DISTRIBUTION
                else:
                    score = self.OOS

                # Distrubtion
                self.common.write_to_db_result_new_tables(fk=self.DIST_CATEGORY_LVL1, numerator_id=result.product_fk,
                                                          numerator_result=score,
                                                          result=score, denominator_id=result.category_fk,
                                                          denominator_result=1, score=score, score_after_actions=score)
                if score == self.OOS:
                    # OOS
                    self.common.write_to_db_result_new_tables(fk=self.OOS_CATEGORY_LVL1, numerator_id=result.product_fk,
                                                              numerator_result=score,
                                                              result=score, denominator_id=result.category_fk,
                                                              denominator_result=1, score=score,
                                                              score_after_actions=score)

            category_list = lvl3_with_cat['category_fk'].unique()
            for cat in category_list:
                lvl3_result_cat = lvl3_with_cat[lvl3_with_cat["category_fk"] == cat]
                lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result_cat)
                for result in lvl2_result.itertuples():
                    denominator_res = result.total
                    res = np.divide(float(result.passes), float(denominator_res))
                    # Distrubtion
                    self.common.write_to_db_result_new_tables(fk=self.DIST_CATEGORY_LVL2,
                                                              numerator_id=self.MANUFACTURER_FK,
                                                              numerator_result=result.passes,
                                                              denominator_id=cat,
                                                              denominator_result=denominator_res,
                                                              result=res, score=res, score_after_actions=res)

                    # OOS
                    self.common.write_to_db_result_new_tables(fk=self.OOS_CATEGORY_LVL2,
                                                              numerator_id=self.MANUFACTURER_FK,
                                                              numerator_result=denominator_res - result.passes,
                                                              denominator_id=cat,
                                                              denominator_result=denominator_res,
                                                              result=1 - res, score=(1 - res),
                                                              score_after_actions=1 - res)
        return

    def store_assortment_calculation(self, lvl3_result):
        """
        This function calculates the KPI results.
        """

        for result in lvl3_result.itertuples():
            if result.in_store == 1:
                score = self.DISTRIBUTION
            else:
                score = self.OOS

            # Distrubtion
            self.common.write_to_db_result_new_tables(fk=self.DIST_STORE_LVL1, numerator_id=result.product_fk,
                                                      numerator_result=score,
                                                      result=score, denominator_id=self.store_id,
                                                      denominator_result=1, score=score)
            if score == self.OOS:
                # OOS
                self.common.write_to_db_result_new_tables(fk=self.OOS_STORE_LVL1, numerator_id=result.product_fk,
                                                          numerator_result=score,
                                                          result=score, denominator_id=self.store_id,
                                                          denominator_result=1, score=score,
                                                          score_after_actions=score)

        if not lvl3_result.empty:
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if not pd.isnull(result.target) and not pd.isnull(result.group_target_date) and result.group_target_date <= self.assortment.current_date:
                    denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res))
                # Distrubtion
                self.common.write_to_db_result_new_tables(fk=self.DIST_STORE_LVL2, numerator_id=self.MANUFACTURER_FK,
                                                          denominator_id=self.store_id,
                                                          numerator_result=result.passes,
                                                          denominator_result=denominator_res,
                                                          result=res, score=res, score_after_actions=res)

                # OOS
                self.common.write_to_db_result_new_tables(fk=self.OOS_STORE_LVL2,
                                                          numerator_id=self.MANUFACTURER_FK,
                                                          numerator_result=denominator_res - result.passes,
                                                          denominator_id=self.store_id,
                                                          denominator_result=denominator_res,
                                                          result=1 - res, score=1 - res, score_after_actions=1 - res)
        return
