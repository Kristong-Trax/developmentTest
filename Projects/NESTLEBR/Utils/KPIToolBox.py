
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class NESTLEBRToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_secondary_manufacturer_out_of_category()
        return

    def calculate_secondary_manufacturer_out_of_category(self):
        relevant_scif = self.scif[self.scif['included_in_secondary_shelf_report'] == 'Y']

        denominator_results = relevant_scif.groupby('sub_category_fk', as_index=False)[
                ['facings_ign_stack']].sum().rename(columns={'facings_ign_stack': 'denominator_result'})

        numerator_result = relevant_scif.groupby(['sub_category_fk', 'manufacturer_fk'], as_index=False)[
            ['facings_ign_stack']].sum().rename(columns={'facings_ign_stack': 'numerator_result'})

        results = numerator_result.merge(denominator_results)
        results['result'] = ((results['numerator_result'] / results['denominator_result']) * 100)

        for index, row in results.iterrows():
            result_dict = self.build_dictionary_for_db_insert(
                kpi_name='SECONDARY_FACINGS_SOS_MANUFACTURER_OUT_OF_CATEGORY_IN_WHOLE_STORE',
                numerator_id=row['manufacturer_fk'], denominator_id=row['sub_category_fk'],
                numerator_result=row['numerator_result'], denominator_result=row['denominator_result'],
                result=row['result'], score=row['result'], score_after_actions=row['result'])
            self.common.write_to_db_result(**result_dict)

    def build_dictionary_for_db_insert(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                       denominator_id=0, denominator_result=0, score=0, score_after_actions=0,
                                       denominator_result_after_actions=None, numerator_result_after_actions=0,
                                       weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=None,
                                       target=None,
                                       identifier_parent=None, identifier_result=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.common.get_kpi_fk_by_kpi_name(kpi_name)
            else:
                insert_params['fk'] = fk
            insert_params['numerator_id'] = numerator_id
            insert_params['numerator_result'] = numerator_result
            insert_params['denominator_id'] = denominator_id
            insert_params['denominator_result'] = denominator_result
            insert_params['result'] = result
            insert_params['score'] = score
            if target:
                insert_params['target'] = target
            if denominator_result_after_actions:
                insert_params['denominator_result_after_actions'] = denominator_result_after_actions
            if context_id:
                insert_params['context_id'] = context_id
            if identifier_parent:
                insert_params['identifier_parent'] = identifier_parent
                insert_params['should_enter'] = True
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None

    def commit_results_data(self):
        self.common.commit_results_data()
