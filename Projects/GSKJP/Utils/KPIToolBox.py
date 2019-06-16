
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
import pandas as pd
import os
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from Trax.Utils.Logging.Logger import Log

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class GSKJPToolBox:
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
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)
        self.blocking_generator = Block(self.data_provider)
        self.assortment = self.gsk_generator.get_assortment_data_provider()
        self.store_info = self.data_provider['store_info']
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        self.get_store_target()# choosing the policy
        if self.targets.empty:
            Log.warning('There is no target policy matching this store ')
        else:
            self.brand_blocking()
        assortment_store_dict = self.gsk_generator.availability_store_function()
        # self.common.save_json_to_new_tables(assortment_store_dict)
        #
        # assortment_category_dict = self.gsk_generator.availability_category_function()
        # self.common.save_json_to_new_tables(assortment_category_dict)
        #
        # assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function()
        # self.common.save_json_to_new_tables(assortment_subcategory_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # self.common.commit_results_data()
        return

    def position_shelf(self):

        brand_fk = 2
        policy = self.targets[self.targets['brand_fk'] == brand_fk]
        if policy.empty:
            Log.warning('There is no target policy matching brand')  # adding branf name
            return
        shelf_from_bottom = policy['shelf']
        threshold = policy['position_target']
        target = 0.25
        df = pd.merge(self.match_product_in_scene, self.all_products, on="product_fk")
        df = df[df['stacking_layer'] == 1]
        brand_df = df[df['brand_fk'] == brand_fk]
        shelf_df = brand_df[brand_df['shelf_number_from_bottom'].isin(shelf_from_bottom)]
        result = shelf_df.shape[0] / brand_df.shape[0]
        score = target if result >= threshold else 0

        # results_df.append({'fk': kpi_product_pk, 'numerator_id': prod, 'denominator_id': denominator_fk,
        #                'denominator_result': result, 'numerator_result': 1, 'result':
        #                    result, 'score': score, 'target': kpi_target, 'identifier_parent': identifier_parent,
        #                'should_enter': True})



    def brand_blocking(self):

        results_df = []
        df_block = self.scif
        brands = df_block['brand_fk'].dropna().unique()
        template_name = 'Oral Main Shelf'# figure out which template name should I use
        stacking_param = True# false
        target = 1
        kpi_target = 0.25
        product_filters = {'product_type': ['POS', 'Empty', 'Irrelevant']} # from Data file
        for brand in brands:
            policy = self.targets[self.targets['brand_fk'] == brand]
            if policy.empty:
                Log.warning('There is no target policy matching brand')# adding branf name
                continue
            result = self.blocking_generator.network_x_block_together(location={'template_name': template_name},
                                population={'brand_fk': [brand]},
                                additional={'minimum_block_ratio': 1,
                                            'allowed_products_filters': product_filters,
                                            'calculate_all_scenes': False,
                                            'include_stacking': stacking_param,
                                            'check_vertical_horizontal': True,
                                            'minimum_facing_for_block': policy['block_target']})
            if result[result['is_block'] == True].empty:
                result = 0
            score = result * kpi_target

            # results_df.append({'fk': kpi_product_pk, 'numerator_id': prod, 'denominator_id': denominator_fk,
            #                        'denominator_result': result, 'numerator_result': 1, 'result':
            #                            result, 'score': score, 'target': kpi_target, 'identifier_parent': identifier_parent,
            #                        'should_enter': True})

    def PLN_MSL_score(self):
        if self.assortment is None:
            return
        lvl3_assort = self.assortment.calculate_lvl3_assortment()
        lvl3_assort ### merge lvl3 

    def get_store_target(self):

        parameters = ['additional_attribute_1', 'additional_attribute_2', 'store_name', 'adress_city', 'region_fk']
        for param in parameters:
            if param in self.targets.columns:
                self.targets = self.targets[(self.targets[param] == self.store_info[param][0].encode('utf-8')) |
                                            (self.targets[param] == '')]





    # def filter_df(self):
    #     return self.scif