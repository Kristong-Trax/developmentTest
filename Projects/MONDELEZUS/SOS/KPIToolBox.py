import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import math
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import os
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.MONDELEZUS.Utils.ParseTemplates import parse_template
import datetime


class MONDELEZUSSOSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
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
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        self.kpi_results_queries = []
        self.potential_products = {}
        self.shelf_square_boundaries = {}
        self.average_shelf_values = {}
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.template_info = self.data_provider.all_templates
        self.ignore_stacking = True
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.kpi_new_static_data = self.common.kpi_static_data

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """
        Log.info('Starting SOS calculations')
        self.calculate_facings_sos_sku_out_of_template()
        self.calculate_linear_sos_sku_out_of_template()
        Log.info('Finished SOS calculations')

    def calculate_facings_sos_sku_out_of_template(self):
        try:
            relevant_scif = self.scif[~self.scif['product_type'].isin(['Irrelevant'])]

            denominator_results = relevant_scif.groupby('template_fk', as_index=False)[
                    [self.facings_field]].sum().rename(columns={self.facings_field: 'denominator_result'})

            numerator_result = relevant_scif.groupby(['template_fk', 'product_fk'], as_index=False)[
                [self.facings_field]].sum().rename(columns={self.facings_field: 'numerator_result'})

            results = numerator_result.merge(denominator_results)
            results['result'] = (results['numerator_result'] / results['denominator_result'])
            results['result'].fillna(0, inplace=True)

            for index, row in results.iterrows():
                if row['numerator_result'] > 0:
                    result_dict = self.build_dictionary_for_db_insert(
                        kpi_name='FACINGS_SOS_SKU_OUT_OF_TEMPLATE_IN_WHOLE_STORE',
                        numerator_id=row['product_fk'], denominator_id=row['template_fk'],
                        numerator_result=row['numerator_result'], denominator_result=row['denominator_result'],
                        result=row['result'], score=row['result'], score_after_actions=row['result'])
                    self.common.write_to_db_result(**result_dict)

        except Exception as e:
            Log.info('FACINGS_SOS_SKU_OUT_OF_TEMPLATE calculation failed due to {}'.format(e))

    def calculate_linear_sos_sku_out_of_template(self):
        try:
            relevant_scif = self.scif[~self.scif['product_type'].isin(['Irrelevant'])]

            denominator_results = relevant_scif.groupby('template_fk', as_index=False)[
                ['net_len_ign_stack']].sum().rename(columns={'net_len_ign_stack': 'denominator_result'})

            numerator_result = relevant_scif.groupby(['template_fk', 'product_fk'], as_index=False)[
                ['net_len_ign_stack']].sum().rename(columns={'net_len_ign_stack': 'numerator_result'})

            results = numerator_result.merge(denominator_results)
            results['result'] = (results['numerator_result'] / results['denominator_result'])
            results['result'].fillna(0, inplace=True)

            for index, row in results.iterrows():
                if row['numerator_result'] > 0:
                    result_dict = self.build_dictionary_for_db_insert(
                        kpi_name='LINEAR_SOS_SKU_OUT_OF_TEMPLATE_IN_WHOLE_STORE',
                        numerator_id=row['product_fk'], denominator_id=row['template_fk'],
                        numerator_result=row['numerator_result'], denominator_result=row['denominator_result'],
                        result=row['result'], score=row['result'], score_after_actions=row['result'])
                    self.common.write_to_db_result(**result_dict)

        except Exception as e:
            Log.info('LINEAR_SOS_SKU_OUT_OF_TEMPLATE calculation failed due to {}'.format(e))

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
