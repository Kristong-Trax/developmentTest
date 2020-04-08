from Projects.NESTLEBAKINGUS.Data.LocalConsts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
import numpy as np
import simplejson
import os


__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Nestle Creamers Template V1.7.xlsx')

SHEETS = [Consts.KPIS, Consts.SHELF_COUNT, Consts.SOS, Consts.DISTRIBUTION, Consts.DISTRIBUTION,
          Consts.BASE_MEASUREMENT, Consts.SHELF_POSITION, Consts.XREF_SCENE_TYPE_TO_CATEGORY,
          Consts.SHELF_POSITION_TEMPLATE]
CONVERT_MM_TO_INCHES = 0.0393701


class NESTLEBAKINGUSToolBox(GlobalSessionToolBox):
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.templates = {}
        self.parse_template()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.match_display_in_scene = self.data_provider.match_display_in_scene
        self._filter_display_in_scene()
        self.match_scene_item_facts = pd.merge(self.scif, self.match_product_in_scene, how='right',
                                               left_on=['item_id', 'scene_id'], right_on=['product_fk',
                                                                                          'scene_fk'])  # Merges scif with mpis on product_fk
        self.match_scene_item_facts.rename(columns={'product_fk_x': 'product_fk', 'scene_fk_x': Consts.SCENE_FK},
                                           inplace=True)
        self._filter_scif_and_mpis_by_template_name_scene_type_and_category_name()
        self._filter_scif_by_by_template_name_scene_type_and_category_name()
        # self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

        # self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY] = pd.read_excel(TEMPLATE_PATH,
        #                                                                    sheet_name=Consts.XREF_SCENE_TYPE_TO_CATEGORY)

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
        # Consts.BASE_MEASUREMENT, Consts.SHELF_POSITION, Consts.SOS, Consts.SHELF_COUNT,Consts.DISTRIBUTION
        foundation_kpi_types = [Consts.BASE_MEASUREMENT, Consts.SHELF_POSITION, Consts.SOS, Consts.SHELF_COUNT,
                                Consts.DISTRIBUTION]
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
                if result_data and isinstance(result_data,list):
                    for result in result_data:
                        self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SHELF_COUNT:
            return self._calculate_shelf_count
        elif kpi_type == Consts.SHELF_POSITION:
            return self._calculate_shelf_position
        elif kpi_type == Consts.SOS:
            return self._calculate_sos
        elif kpi_type == Consts.DISTRIBUTION:
            return self._calculate_distribution
        elif kpi_type == Consts.BASE_MEASUREMENT:
            return self._calculate_base_measurement

    def _calculate_base_measurement(self, row):
        if not self.match_display_in_scene.empty:  # there are some session that the mdis is not empty
            kpi_name = row[Consts.KPI_NAME]
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

            mpis = self.data_provider[Data.MATCHES]
            mpis = mpis[mpis.stacking_layer == 1]
            mdis_merged_mcif = pd.merge(self.match_display_in_scene, mpis, how='left',
                                        left_on='scene_fk', right_on='scene_fk')
            unique_scenefks_in_mdis_merged_mcif = np.unique(mdis_merged_mcif.scene_fk)

            # Have to do this as they are scenes with no display fks. So the line of code below filters out any scene
            # with no display fk
            filtered_unique_scenefks_in_mdis_merged_mpis = unique_scenefks_in_mdis_merged_mcif[
                ~ np.isnan(unique_scenefks_in_mdis_merged_mcif)]

            # this is how the ids are store for display fk in custom entity
            # The reason we are using custom entity is because there may be a case where a scene has two types of displays
            # In this case, we have use and save an id of three which represent a scene with both display fks
            # Because of this we can not refer to the display table in sql
            display_fk_dictionary = {1: 17, 2: 18, 3: 19}
            result_dict_list = []
            for unique_scene in filtered_unique_scenefks_in_mdis_merged_mpis:
                unique_displayfks_in_scene = np.unique(
                    mdis_merged_mcif[mdis_merged_mcif.scene_fk == unique_scene].display_fk)
                display_fk_for_scene = unique_displayfks_in_scene[0] if len(unique_displayfks_in_scene) == 1 else 3
                display_fk_id = self.get_display_fk_id(display_fk_dictionary,display_fk_for_scene)
                relevant_mpis = mpis[mpis.scene_fk.isin([unique_scene])]
                for unique_bay in set(relevant_mpis.bay_number):
                    useful_mcif = relevant_mpis[relevant_mpis.bay_number.isin([unique_bay])]
                    max_width = useful_mcif.groupby(by='shelf_number').sum()[
                                    'width_mm_advance'].max() * CONVERT_MM_TO_INCHES
                    result_dict = {'kpi_fk': kpi_fk, 'numerator_id': display_fk_id,
                                   'denominator_id': unique_bay, 'context_id': unique_scene,
                                   'result': int(max_width)}
                    result_dict_list.append(result_dict)
            return result_dict_list

    def _calculate_shelf_count(self, row):
        """
        Logic of KPI: Get the number of shelves per scene per bay
        The kpi should only run if display fk is 1 or 2
        """

        # Should use logic where self.mcif (merge of scif and mpis) isn't filtered by the category logic
        # Had to do this last minute
        if not self.match_display_in_scene.empty:  # there are some session that the mdis is not empty
            kpi_name = row[Consts.KPI_NAME]
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

            mpis = self.data_provider[Data.MATCHES]
            mdis_merged_mcif = pd.merge(self.match_display_in_scene, mpis, how='left',
                                        left_on='scene_fk', right_on='scene_fk')
            unique_scenefks_in_mdis_merged_mcif = np.unique(mdis_merged_mcif.scene_fk)

            # Have to do this as they are scenes with no display fks. So the line of code below filters out any scene
            # with no display fk
            filtered_unique_scenefks_in_mdis_merged_mpis = unique_scenefks_in_mdis_merged_mcif[
                ~ np.isnan(unique_scenefks_in_mdis_merged_mcif)]
            # this is how the ids are store for display fk in custom entity
            # The reason we are using custom entity is because there may be a case where a scene has two types of displays
            # In this case, we have use and save an id of three which represent a scene with both display fks
            # Because of this we can not refer to the display table in sql
            display_fk_dictionary = {1: 17, 2: 18, 3: 19}
            result_dict_list = []
            for unique_scene in filtered_unique_scenefks_in_mdis_merged_mpis:
                unique_displayfks_in_scene = np.unique(
                    mdis_merged_mcif[mdis_merged_mcif.scene_fk == unique_scene].display_fk)
                display_fk_for_scene = unique_displayfks_in_scene[0] if len(unique_displayfks_in_scene) == 1 else 3
                display_fk_id = self.get_display_fk_id(display_fk_dictionary,display_fk_for_scene)
                relevant_mcif = mpis[mpis.scene_fk.isin([unique_scene])]
                for unique_bay in set(relevant_mcif.bay_number):
                    useful_mcif = relevant_mcif[relevant_mcif.bay_number.isin([unique_bay])]
                    highest_shelf_number_in_bay = useful_mcif.shelf_number.max()
                    result_dict = {'kpi_fk': kpi_fk, 'numerator_id': display_fk_id,
                                   'denominator_id': unique_bay, 'context_id': unique_scene,
                                   'result': highest_shelf_number_in_bay}
                    result_dict_list.append(result_dict)
            return result_dict_list

    def _calculate_distribution(self, row):
        """
        Logic of the kpi. The kpi check if product exists in the scif.
        If it exists, Save the facing with stacking in result and 1 in score
        If not exists, Save 0 in score and result
        """
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_product_fk = self._sanitize_values(row[Consts.NUMERATOR_VALUE_1])

        bool_array_present_products_fk_in_session = pd.np.in1d(relevant_product_fk,
                                                               self.scif.product_fk.unique())
        present_products_fk_in_session_index = pd.np.flatnonzero(bool_array_present_products_fk_in_session)
        present_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[present_products_fk_in_session_index]
        absent_products_fk_in_session_index = pd.np.flatnonzero(~ bool_array_present_products_fk_in_session)
        absent_products_fk_in_session = pd.np.array(relevant_product_fk).ravel()[absent_products_fk_in_session_index]

        result_dict_list = []
        for present_product_fk in present_products_fk_in_session:
            result = self.scif[self.scif.product_fk.isin([present_product_fk])].facings.sum()
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': present_product_fk,
                           'denominator_id': self.store_id,
                           'result': result, 'score': 1}
            result_dict_list.append(result_dict)

        for absent_products_fk_in_session in absent_products_fk_in_session:
            result = 0
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': absent_products_fk_in_session,
                           'denominator_id': self.store_id,
                           'result': result, 'score': 0}
            result_dict_list.append(result_dict)
        return result_dict_list

    def _calculate_shelf_position(self, row):
        if not self.match_display_in_scene.empty:  # there are some session that the mdis is not empty
            kpi_name = row[Consts.KPI_NAME]
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

            shelf_position_dict = {'Bottom': 13, 'Middle': 14, 'Eye': 15, 'Top': 16}
            result_dict_list = []

            mdis_merged_mcif = pd.merge(self.match_display_in_scene, self.match_scene_item_facts, how='left',
                                        left_on='scene_fk', right_on='scene_id')
            mdis_merged_mcif.dropna(subset=['scene_id'], inplace=True)
            relevant_mcif = mdis_merged_mcif[
                ~ mdis_merged_mcif.product_type.isin(['Other'])] if row[Consts.SKU_RELEVANT] == 'Y' else \
                mdis_merged_mcif[mdis_merged_mcif.product_type.isin(['Other'])]

            if relevant_mcif.empty:
                return result_dict_list

            if row[Consts.IGNORE_STACKING]:
                relevant_mcif = relevant_mcif[relevant_mcif.stacking_layer == 1]

            for unique_scene in set(relevant_mcif.scene_id):
                relevant_mcif_filtered = self._filter_df(self.match_scene_item_facts, {Consts.SCENE_ID: unique_scene})
                for unique_item in set(relevant_mcif_filtered[row[
                    Consts.DENOMINATOR_TYPE_FK]]):  # this can be a product_fk or category_fk depending on the kpi
                    item_filtered_mcif = self._filter_df(relevant_mcif_filtered, {row[
                                                                                      Consts.DENOMINATOR_TYPE_FK]: unique_item})
                    for unique_shelf in set(item_filtered_mcif.shelf_number):
                        final_mcif = self._filter_df(item_filtered_mcif, {'shelf_number': unique_shelf})
                        numerator_result = round(
                            final_mcif[row[Consts.OUTPUT]].sum() * CONVERT_MM_TO_INCHES) if pd.notna(
                            row[Consts.OUTPUT]) else final_mcif.facings.count()

                        # this method uses the 'shelf position template' sheet to derive the shelf position id
                        shelf_position_id = self._get_shelf_position_id(unique_scene, current_shelf=unique_shelf,
                                                                        shelf_position_dict=shelf_position_dict)

                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': shelf_position_id,
                                       'denominator_id': unique_item, 'context_id': unique_scene,
                                       'numerator_result': numerator_result}
                        result_dict_list.append(result_dict)
            return result_dict_list

    def _calculate_sos(self, row):
        """
        Have to save sos by scene
        """
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        result_dict_list = []
        for unique_scene_fk in set(self.scif.scene_fk):
            relevant_scif = self._filter_df(self.match_scene_item_facts, {Consts.SCENE_FK: unique_scene_fk})
            if pd.notna(row[Consts.FILTER_DENOMINATOR]):
                # denominator_result = relevant_scif.net_len_ign_stack.sum() if relevant_scif.empty else 1
                for unique_category_fk in set(relevant_scif[row[Consts.FILTER_DENOMINATOR]]):
                    relevant_scif_filtered = self._filter_df(relevant_scif,
                                                             {row[Consts.FILTER_DENOMINATOR]: unique_category_fk})
                    denominator_result = relevant_scif_filtered[row[Consts.OUTPUT]].sum() if not row[
                        Consts.LINEAR_RELEVANT] else relevant_scif_filtered[
                                                         row[Consts.OUTPUT]].sum() * CONVERT_MM_TO_INCHES
                    for unique_product_fk in set(relevant_scif_filtered[Consts.PRODUCT_FK]):
                        final_scif = self._filter_df(relevant_scif_filtered, {Consts.PRODUCT_FK: unique_product_fk})
                        numerator_result = final_scif[row[Consts.OUTPUT]].sum() if not row[Consts.LINEAR_RELEVANT] else \
                            final_scif[row[Consts.OUTPUT]].sum() * CONVERT_MM_TO_INCHES
                        result = (float(numerator_result) / denominator_result) * 100
                        result_dict = {'kpi_fk': kpi_fk, 'numerator_id': unique_product_fk,
                                       'denominator_id': unique_category_fk, 'context_id': unique_scene_fk,
                                       'numerator_result': numerator_result, 'denominator_result': denominator_result,
                                       'result': result}
                        result_dict_list.append(result_dict)
            else:
                denominator_result = relevant_scif[row[Consts.OUTPUT]].sum() if not row[
                    Consts.LINEAR_RELEVANT] else relevant_scif[
                                                     row[Consts.OUTPUT]].sum() * CONVERT_MM_TO_INCHES

                for unique_category_fk in set(relevant_scif['category_fk']):
                    relevant_scif_filtered = self._filter_df(relevant_scif, {Consts.CATEGORY_FK: unique_category_fk})
                    numerator_result = relevant_scif_filtered[row[Consts.OUTPUT]].sum() if not row[
                        Consts.LINEAR_RELEVANT] else relevant_scif_filtered[
                                                         row[Consts.OUTPUT]].sum() * CONVERT_MM_TO_INCHES
                    result = (float(numerator_result) / denominator_result) * 100
                    result_dict = {'kpi_fk': kpi_fk, 'numerator_id': unique_category_fk,
                                   'denominator_id': self.store_id, 'context_id': unique_scene_fk,
                                   'numerator_result': numerator_result, 'denominator_result': denominator_result,
                                   'result': result}
                    result_dict_list.append(result_dict)
        return result_dict_list

    def _get_shelf_position_id(self, unique_scene, current_shelf, shelf_position_dict):
        '''
        For the self position kpi
        :return: Uses the shelf position in the scene and bay and return the id of the shelf based on the shelf_position_dict
        The dictionary is based on the custom entity table
        '''
        max_shelf_position = self.match_scene_item_facts[
            (self.match_scene_item_facts.scene_id.isin([unique_scene]))].shelf_number.max()

        return shelf_position_dict[self.templates[Consts.SHELF_POSITION_TEMPLATE].loc[
            self.templates[Consts.SHELF_POSITION_TEMPLATE]['Num Shelves'] == max_shelf_position, current_shelf].iat[0]]

    def _filter_display_in_scene(self):
        ''''
        For the shelf count kpi, we need to make sure that we can't take scenes that have a display fk of 3 or 4.
        '''

        mdis_array = self.match_display_in_scene[['scene_fk', 'display_fk']].to_numpy()
        filter = ['3', '4']

        index_of_mdis_array_where_display_fk_is_3_or_4 = np.in1d(mdis_array[:, 1], filter)
        scenes_with_not_relevant_display_fks = mdis_array[index_of_mdis_array_where_display_fk_is_3_or_4][:,
                                               0]  # gets the scenes that have a display fk of 3 or 4

        if scenes_with_not_relevant_display_fks.size > 0:  # logic: if array is not empty
            unique_scenes_not_relevant_for_display = np.unique(scenes_with_not_relevant_display_fks)
            self.match_display_in_scene = self.match_display_in_scene[
                ~ self.match_display_in_scene.scene_fk.isin(unique_scenes_not_relevant_for_display)]

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
            -1)  # gets the index of match scene item facts where the category fk and template  fk match that derived from self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY]

        self.match_scene_item_facts = self.match_scene_item_facts[
            filter_scif_by_relevant_selftemplate_by_category_fk_and_template_fk]

    def _filter_scif_by_by_template_name_scene_type_and_category_name(self):
        ''' Logic of this project
                Have to filter the scif by only taking in products with category fks that match the template fk (scene type).
                Using self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY] as the refrence.'''
        current_category_and_template = self.scif[['template_fk', 'category_fk']].to_numpy()
        relevant_selftemplate_by_category_fk_and_template_fk = self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY][
            ['template_fk', 'category_fk']].to_numpy()
        filter_scif_by_relevant_selftemplate_by_category_fk_and_template_fk = (
                current_category_and_template[:, None] == relevant_selftemplate_by_category_fk_and_template_fk).all(
            -1).any(
            -1)  # gets the index of scif where the category fk and template  fk match that derived from self.templates[Consts.XREF_SCENE_TYPE_TO_CATEGORY]
        self.scif = self.scif[filter_scif_by_relevant_selftemplate_by_category_fk_and_template_fk]

    @staticmethod
    def get_display_fk_id(display_fk_dictionary, display_fk_for_scene):
        ''''The display_fk is saved a part of custom entity because of this '''
        relevant_display_fk_to_save = display_fk_dictionary.get(display_fk_for_scene, 3)
        return relevant_display_fk_to_save

    @staticmethod
    def _filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    @staticmethod
    def _sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items


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
