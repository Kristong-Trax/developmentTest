from Projects.NESTLEBAKINGUS.Data.LocalConsts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import numpy as np
import simplejson
import os

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Nestle Creamers Template V1.1.xlsx')

SHEETS = [Consts.KPIS, Consts.SHELF_COUNT, Consts.SOS, Consts.DISTRIBUTION, Consts.DISTRIBUTION,
          Consts.BASE_MEASUREMENT, Consts.SHELF_POSITION]


class NESTLEBAKINGUSToolBox(GlobalSessionToolBox):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.parse_template()
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.match_scene_item_facts = pd.merge(self.scif, self.match_product_in_scene, how='right',
                                               left_on=['item_id', 'scene_id'], right_on=['product_fk',
                                                                                          'scene_fk'])  # Merges scif with mpis on product_fk
        self._filter_scif_and_mpis_by_template_name_scene_type_and_category_name()
        # self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

        self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY] = pd.read_excel(TEMPLATE_PATH,
                                                                           sheet_name=Consts.XREF_SCENE_TYPE_TO_CATEGORY)

    def _filter_scif_and_mpis_by_template_name_scene_type_and_category_name(self):
        ''' Logic of this project
        Have to filter the scif by only taking in products with category fks that match the template fk (scene type).
        Using self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY] as the refrence.'''

        current_category_and_template = self.match_scene_item_facts[['template_fk', 'category_fk']].to_numpy()
        relevant_selftemplate_by_category_fk_and_template_fk = self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY][
            ['template_fk', 'category_fk']].to_numpy()
        filter_scif_by_relevant_selftemplate_by_category_fk_and_template_fk = (
                current_category_and_template[:, None] == relevant_selftemplate_by_category_fk_and_template_fk).all(
            -1).any(
            -1)  # gets the index of scif where the category fk and template  fk match that derived from self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY]

        self.match_scene_item_facts = self.match_scene_item_facts[
            filter_scif_by_relevant_selftemplate_by_category_fk_and_template_fk]

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.write_to_db(**result)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        relevant_kpi_template = self.templates[Consts.KPIS]
        foundation_kpi_types = [Consts.SHELF_COUNT, Consts.SHELF_POSITION]
        foundation_kpi_template = relevant_kpi_template[
            relevant_kpi_template[Consts.KPI_TYPE].isin(foundation_kpi_types)]

        self._calculate_kpis_from_template(foundation_kpi_template)
        self.save_results_to_db()
        return

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            try:
                kpi_rows = self.templates[row[Consts.KPI_TYPE]][
                    self.templates[row[Consts.KPI_TYPE]][Consts.KPI_NAME].str.encode('utf-8') == row[
                        Consts.KPI_NAME].encode('utf-8')]
            except IndexError:
                pass
            for index, kpi_row in kpi_rows.iterrows():
                result_data = calculation_function(kpi_row)
                if result_data:
                    for result in result_data:
                        self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        # if kpi_type == Consts.SOS:
        #     return self.calculate_sos
        # elif kpi_type == Consts.DISTRIBUTION:
        #     return self.calculate_distribution
        # elif kpi_type == Consts.ADJACENCY_BRAND_WITHIN_BAY:
        #     return self.calculate_adjacency_brand
        # elif kpi_type == Consts.ADJACENCY_CATEGORY_WITHIN_BAY:
        #     return self.calculate_adjacency_category
        # elif kpi_type == Consts.LEAD_ANCHOR_BY_BAY:
        #     return self.calculate_lead_anchor
        if kpi_type == Consts.SHELF_COUNT:
            return self._calculate_shelf_count
        elif kpi_type == Consts.SHELF_POSITION:
            return self._calculate_shelf_position

    def _calculate_shelf_count(self, row):
        '''
        Logic of KPI: Get the number of shelves per scene per bay
        The kpi should only run if display fk is 1 or 2
        '''

        # Need to write the logic for display fk
        # 'numerator_id': numerator_id

        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        result_dict_list = []
        for unique_scene in set(self.match_scene_item_facts.scene_id):
            relevant_mcif = self.match_scene_item_facts[self.match_scene_item_facts.scene_id.isin([unique_scene])]
            for unique_bay in set(relevant_mcif.bay_number):
                useful_mcif = relevant_mcif[relevant_mcif.bay_number.isin([unique_bay])]
                highest_shelf_number_in_bay = useful_mcif.bay_number.max()
                result_dict = {'kpi_fk': kpi_fk,
                               'denominator_id': unique_bay, 'context_id': unique_scene,
                               'result': highest_shelf_number_in_bay}
                result_dict_list.append(result_dict)
            return result_dict_list

    def _calculate_shelf_position(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        result_dict_list = []
        relevant_mcif = self.match_scene_item_facts[
            ~ self.match_scene_item_facts.product_type.isin(['Other'])] if row[Consts.SKU_RELEVANT] == 'Y' else \
        self.match_scene_item_facts[self.match_scene_item_facts.product_type.isin(['Other'])]

        if relevant_mcif.empty:
            return result_dict_list

        for unique_scene in set(relevant_mcif.scene_id):
            # relevant_mcif = self.match_scene_item_facts[self.match_scene_item_facts.scene_id.isin([unique_scene])]
            a =1


'''This code was previously used to for the nestle baking us however we do not use it anymore
We are using the project folder for Nestle Dairy'''
# from Trax.Algo.Calculations.Core.DataProvider import Data
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
# import pandas as pd
# import os
# from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# # from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# # from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# # from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# # from KPIUtils_v2.Calculations.SOSCalculations import SOS
# # from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# # from KPIUtils_v2.Calculations.SurveyCalculations import Survey
# # from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
# __author__ = 'huntery'
# KPI_RESULT = 'report.kpi_results'
# KPK_RESULT = 'report.kpk_results'
# KPS_RESULT = 'report.kps_results'
# class NESTLEBAKINGUSToolBox:
#     LEVEL1 = 1
#     LEVEL2 = 2
#     LEVEL3 = 3
#     def __init__(self, data_provider, output):
#         self.output = output
#         self.data_provider = data_provider
#         self.common = Common(data_provider)
#         self.project_name = self.data_provider.project_name
#         self.session_uid = self.data_provider.session_uid
#         self.products = self.data_provider[Data.PRODUCTS]
#         self.all_products = self.data_provider[Data.ALL_PRODUCTS]
#         self.match_product_in_scene = self.data_provider[Data.MATCHES]
#         self.visit_date = self.data_provider[Data.VISIT_DATE]
#         self.session_info = self.data_provider[Data.SESSION_INFO]
#         self.scene_info = self.data_provider[Data.SCENES_INFO]
#         self.store_info = self.data_provider[Data.STORE_INFO]
#         self.store_id = self.data_provider[Data.STORE_FK]
#         self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
#         self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
#         self.kpi_static_data = self.common.get_kpi_static_data()
#         self.kpi_results_queries = []
#         self.assortment = Assortment(self.data_provider, common=self.common)
#     def main_calculation(self, *args, **kwargs):
#         """
#         This function calculates the KPI results.
#         """
#         self.calculate_assortment()
#         return
#     def calculate_assortment(self):
#         self.assortment.main_assortment_calculation()
#         store_assortment = self.assortment.store_assortment
#         if store_assortment.empty:
#             return
#         kpi_fk = store_assortment['kpi_fk_lvl3'].iloc[0]
#         assortment_fk = store_assortment['assortment_group_fk'].iloc[0]
#         assortment_products = store_assortment['product_fk'].tolist()
#         extra_products = self.scif[(~self.scif['product_fk'].isin(assortment_products)) &
#                                    (self.scif['facings'] > 0) &
#                                    (~self.scif['product_type'].isin(['Empty', 'Irrelevant', 'Other']))]
#         self._save_extra_product_results(extra_products, kpi_fk, assortment_fk)
#     def _save_extra_product_results(self, extra_products, kpi_fk, assortment_fk):
#         for i, row in extra_products.iterrows():
#             result = 2  # for extra
#             self.common.write_to_db_result_new_tables(kpi_fk, row['product_fk'], result, 100,
#                                                       denominator_id=assortment_fk, denominator_result=1)
#     def commit_results_data(self):
#         self.common.commit_results_data_to_new_tables()
