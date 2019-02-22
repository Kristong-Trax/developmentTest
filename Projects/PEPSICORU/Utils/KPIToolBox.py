import os
import numpy as np
import pandas as pd
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Projects.PEPSICORU.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.Common import Common as CommonV1

from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'idanr'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class PEPSICORUToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v1 = CommonV1(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.visit_type = self.store_info[Const.ADDITIONAL_ATTRIBUTE_2].values[0]
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.loc[~(self.scif[Const.PRODUCT_TYPE] == Const.IRRELEVANT)]    # Vitaly's request
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.toolbox = GENERALToolBox(data_provider)
        self.assortment = Assortment(self.data_provider, self.output, common=self.common_v1)
        if not self.scif.empty:
            self.pepsico_fk = self.get_relevant_pk_by_name(Const.MANUFACTURER, Const.PEPSICO)
            self.categories_to_calculate = self.get_relevant_categories_for_session()
            self.main_shelves = self.get_main_shelves()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_share_of_shelf()
        self.calculate_count_of_displays()
        self.calculate_assortment()

    def get_main_shelves(self):
        """
        This function returns a list with the main shelves of this session
        """
        main_shelves_template_groups = [group for group in self.scif[Const.TEMPLATE_GROUP].unique().tolist() if
                                        Const.MAIN_SHELF in group.upper()]
        main_shelves = self.scif[self.scif[Const.TEMPLATE_GROUP].isin(main_shelves_template_groups)][
            Const.TEMPLATE_NAME].unique().tolist()
        return main_shelves

    def get_main_shelf_by_category(self, current_category):
        """
        This function gets a category and return the relevant scene type for the SOS
        :param current_category: One of the product's categories. E.g: Snacks.
        :return: The relevant scene type to the current category
        """
        main_shelves_for_category = []
        for main_shelf in self.main_shelves:
            if current_category.upper() in main_shelf.upper():
                main_shelves_for_category.append(main_shelf)
        return main_shelves_for_category

    @staticmethod
    def get_category_from_template_name(template_name):
        """
        This function gets a template name (scene_type) and return it's relevant category.
        :param template_name: The scene type.
        :return: category name
        """
        if Const.SNACKS.upper() in template_name.upper():
            return Const.SNACKS
        elif Const.BEVERAGES.upper() in template_name.upper():
            return Const.BEVERAGES
        elif Const.JUICES.upper() in template_name.upper():
            return Const.JUICES
        else:
            Log.warning("Couldn't find a matching category for template name = {}".format(template_name))
            return None

    def get_relevant_categories_for_session(self):
        """
        This function returns a list of the relevant categories according to the store type.
        The parameter additional_attribute_2 defines the visit type for each store.
        We have 3 types: Visit LRB (Beverages and Juices), Visit Snack and Visit (= All of them).
        The function is doing intersection between the categories in SCIF and the categories by store type.
        :return: List of the relevant categories
        """
        categories_in_scif = self.scif[Const.CATEGORY].unique().tolist()
        if None in categories_in_scif:
            categories_in_scif.remove(None)
        if not categories_in_scif:
            Log.warning("No categories at scene item facts!")
            return []

        store_type = self.store_info[Const.ADDITIONAL_ATTRIBUTE_2].values[0]
        if not store_type:
            Log.warning("Invalid additional_attribute_2 for store id = {}".format(self.store_id))
            return []
        if Const.SNACKS.upper() in store_type.upper():
            relevant_categories = [Const.SNACKS]
        elif Const.LRB.upper() in store_type.upper():
            relevant_categories = [Const.JUICES, Const.BEVERAGES]
        else:
            relevant_categories = [Const.SNACKS, Const.JUICES, Const.BEVERAGES]
        categories_for_session = list(set(relevant_categories).intersection(set(categories_in_scif)))
        if not categories_for_session:
            Log.warning("There aren't matching categories in scif for this store.")
        return categories_for_session

    def get_relevant_sub_categories_for_category(self, category):
        """
        This function returns a list of the relevant categories according to the scene_types in the session
        :param category: The relevant category
        :return: List of the relevant sub categories for this category
        """
        filtered_scif = self.scif.loc[
            (self.scif[Const.CATEGORY] == category) & (self.scif[Const.MANUFACTURER_NAME] == Const.PEPSICO) & (
                self.scif[Const.TEMPLATE_NAME].isin(self.main_shelves))]
        sub_categories = filtered_scif[Const.SUB_CATEGORY].unique().tolist()
        if None in sub_categories:
            sub_categories.remove(None)
        if not sub_categories:
            Log.warning("No relevant sub categories for category = {}".format(category))
        return sub_categories

    def get_relevant_brands_for_sub_category(self, sub_category):
        """
        This function returns a list of the relevant categories according to the scene_types in the session
        :param sub_category: The relevant sub category
        :return: List of the relevant brands for this category
        """
        filtered_scif = self.scif.loc[
            (self.scif[Const.SUB_CATEGORY] == sub_category) & (self.scif[Const.MANUFACTURER_NAME] == Const.PEPSICO) &
            (self.scif[Const.TEMPLATE_NAME].isin(self.main_shelves))]
        brands_list = filtered_scif[Const.BRAND_NAME].unique().tolist()
        if None in brands_list:
            brands_list.remove(None)
        if not brands_list:
            Log.warning("No relevant brands for sub category = {}".format(sub_category))
        return brands_list

    def get_relevant_pk_by_name(self, filter_by, filter_param):
        """
        This function gets a filter name and returns the relevant pk.
        If the filter_by is equal to category it will be the field name because in SCIF there isn't category_name
        :param filter_by: filter by name E.g: 'category', 'brand'.
        :param filter_param: The param to filter by. E.g: if filter_by = 'category', filter_param could be 'Snack'
        :return: The relevant pk
        """
        pk_field = filter_by + Const.FK
        field_name = filter_by + Const.NAME if Const.CATEGORY not in filter_by else filter_by
        return self.scif.loc[self.scif[field_name] == filter_param][pk_field].values[0]

    def get_target_for_count_of_displays(self):
        """
        This function reads the project's template and returns the targets for all of the levels.
        It iterates over the relevant row and aggregate the result per level.
        :return: target_by_store (int), target_by_category (dict), target_by_scene (dict).
        Plus it return the scene_type list from the template
        """
        targets = pd.read_excel(TEMPLATE_PATH).fillna(0)
        store_number_1 = self.store_info['store_number_1'].values[0]
        if not store_number_1:
            Log.warning("No valid store number 1 for store_fk = {}".format(self.store_id))
            return
        targets = targets.loc[targets[Const.STORE_NUMBER_1] == store_number_1]
        has_targets = False
        store_target, category_targets, scene_targets, scene_types_from_template = None, None, None, None
        if not targets.empty:
            has_targets = True
            targets = targets.drop([Const.STORE_NAME, Const.STORE_NUMBER_1], axis=1)
            target_row = (targets.iloc[0])[(targets.iloc[0]) > 0]   # Takes only the ones with target > 0
            scene_types_from_template = targets.columns.tolist()
            scene_types_with_targets = target_row.keys().tolist()
            # Targets by store:
            store_target = target_row.sum()
            # Targets by category and scenes:
            category_targets = {key: 0 for key in self.categories_to_calculate}
            scene_targets = {key: 0 for key in scene_types_from_template}
            for scene_type in scene_types_with_targets:
                for category in category_targets:
                    if category.upper() in scene_type.upper():
                        # category_targets[category] += 1
                        category_targets[category] += target_row[scene_type]
                scene_targets[scene_type] = target_row[scene_type]
        return has_targets, store_target, category_targets, scene_targets, scene_types_from_template

    def get_relevant_scene_types_from_list(self, scene_types_from_template):
        """
        There's a gap between the actual name and the name is the template because of the visit type.
        So this function returns the a dictionary the narrows it.
        :param scene_types_from_template: Scene type list from the template
        :return: A dictionary where the keys are the names from the templates and values are the actual names
        """
        scene_types_dict = dict.fromkeys(scene_types_from_template)
        relevant_templates_for_visit_type = self.all_templates.loc[
            (self.all_templates[Const.ADDITIONAL_ATTRIBUTE_2] == self.visit_type) & (
            ~self.all_templates[Const.TEMPLATE_NAME].isin(self.main_shelves))][Const.TEMPLATE_NAME].unique().tolist()
        for scene_type in scene_types_from_template:
            for template in relevant_templates_for_visit_type:
                if scene_type.upper() in template.upper():
                    scene_types_dict[scene_type] = template
        # Remove irrelevant scene types from the dictionary
        for key in scene_types_dict.keys():
            if not scene_types_dict[key]:
                del scene_types_dict[key]
        return scene_types_dict

    def calculate_count_of_displays(self):
        """
        This function will calculate the Count of # of Pepsi Displays KPI
        :return:
        """
        # Notice! store_target is Integer, scene_type_list is a list and the rest are dictionaries
        has_target, store_target, category_targets, scene_targets, scene_type_list = self.get_target_for_count_of_displays()
        if not store_target:
            Log.warning("No targets were defined for this store (pk = {})".format(self.store_id))
            return
        # Filtering out the main shelves
        if has_target:
            relevant_scenes_dict = self.get_relevant_scene_types_from_list(scene_type_list)
            relevant_template_name_list = relevant_scenes_dict.values()
            filtered_scif = self.scif.loc[self.scif[Const.TEMPLATE_NAME].isin(relevant_template_name_list)]

            display_count_store_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_STORE_LEVEL)
            scene_types_in_store = len(filtered_scif[Const.SCENE_FK].unique())
            identifier_parent_store_level = self.common.get_dictionary(kpi_fk=display_count_store_level_fk)
            count_store_level = 0

            # Calculate count of display - category_level
            display_count_category_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_CATEGORY_LEVEL)
            for category in self.categories_to_calculate:
                current_category_target = category_targets[category]
                if not current_category_target:
                    continue
                category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
                relevant_scenes = [scene_type for scene_type in relevant_template_name_list if
                                   category.upper() in scene_type.upper()]
                filtered_scif_by_cat = filtered_scif.loc[filtered_scif[Const.TEMPLATE_NAME].isin(relevant_scenes)]
                display_count_category_level_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
                                                                                     category=category)
                # scene_types_in_cate = 0
                # result_cat_level = 0
                # if not filtered_scif_by_cat.empty:
                #     scene_types_in_cate = len(filtered_scif_by_cat[Const.SCENE_FK].unique())
                #     result_cat_level = 1.0 if scene_types_in_cate >= current_category_target else scene_types_in_cate / float(
                #         current_category_target)
                # self.common.write_to_db_result(fk=display_count_category_level_fk, numerator_id=self.pepsico_fk,
                #                                numerator_result=scene_types_in_cate,
                #                                denominator_id=category_fk, denominator_result=current_category_target,
                #                                identifier_result=display_count_category_level_identifier,
                #                                identifier_parent=identifier_parent_store_level,
                #                                result=result_cat_level, should_enter=True)
                scene_count_in_cate = 0
                result_cat_level = 0
                if not filtered_scif_by_cat.empty:
                    actual_scene_names_in_cate = filtered_scif_by_cat[Const.TEMPLATE_NAME].unique().tolist()
                    reverse_scene_dict = {}
                    for scene_type, actual_scene_name in relevant_scenes_dict.iteritems():
                        for sc in actual_scene_names_in_cate:
                            if actual_scene_name == sc:
                                reverse_scene_dict[actual_scene_name] = scene_type
                    df = filtered_scif_by_cat[[Const.TEMPLATE_NAME, 'scene_id']].drop_duplicates()
                    df['scene_type'] = df[Const.TEMPLATE_NAME].apply(lambda x: reverse_scene_dict.get(x))
                    by_scene_count_in_cat = df.groupby(['scene_type']).count()
                    for i, row in by_scene_count_in_cat.iterrows():
                        scene_count_in_cate += scene_targets[i] if row[Const.TEMPLATE_NAME]>=scene_targets[i] \
                            else row[Const.TEMPLATE_NAME]
                    result_cat_level = 1.0 if scene_count_in_cate >= current_category_target else scene_count_in_cate / float(
                            current_category_target)
                self.common.write_to_db_result(fk=display_count_category_level_fk, numerator_id=self.pepsico_fk,
                                               numerator_result=scene_count_in_cate,
                                               denominator_id=category_fk, denominator_result=current_category_target,
                                               identifier_result=display_count_category_level_identifier,
                                               identifier_parent=identifier_parent_store_level,
                                               result=result_cat_level, should_enter=True)

            # Calculate count of display - scene_level
            display_count_scene_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_SCENE_LEVEL)
            for scene_type in relevant_scenes_dict.keys():
                scene_type_target = scene_targets[scene_type]
                if not scene_type_target:
                    continue
                actual_scene_name = relevant_scenes_dict[scene_type]
                relevant_category = self.get_category_from_template_name(actual_scene_name)
                relevant_category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, relevant_category)
                scene_type_score = len(
                    filtered_scif[filtered_scif[Const.TEMPLATE_NAME] == actual_scene_name][Const.SCENE_FK].unique())

                result_scene_level = 1.0 if scene_type_score >= scene_type_target else scene_type_score / float(
                    scene_type_target)
                scene_type_fk = self.all_templates.loc[self.all_templates[Const.TEMPLATE_NAME] == actual_scene_name][
                    Const.TEMPLATE_FK].values[0]
                parent_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
                                                               category=relevant_category)
                self.common.write_to_db_result(fk=display_count_scene_level_fk, numerator_id=self.pepsico_fk,
                                               numerator_result=scene_type_score,
                                               denominator_id=relevant_category_fk, denominator_result=scene_type_target,
                                               identifier_parent=parent_identifier, context_id=scene_type_fk,
                                               result=result_scene_level, should_enter=True)
                count_store_level += scene_type_target if scene_type_score >= scene_type_target else scene_type_score

            # Calculate count of display - store_level
            result_store_level = 1.0 if count_store_level >= store_target else count_store_level / float(store_target)
            self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.pepsico_fk,
                                           numerator_result=count_store_level,
                                           denominator_id=self.store_id, denominator_result=store_target,
                                           identifier_result=identifier_parent_store_level,
                                           result=result_store_level, should_enter=True)

    # def calculate_count_of_displays_old(self):
    #     """
    #     This function will calculate the Count of # of Pepsi Displays KPI
    #     :return:
    #     """
    #     # Notice! store_target is Integer, scene_type_list is a list and the rest are dictionaries
    #     store_target, category_targets, scene_targets, scene_type_list = self.get_target_for_count_of_displays()
    #     if not store_target:
    #         Log.warning("No targets were defined for this store (pk = {})".format(self.store_id))
    #         return
    #     # Filtering out the main shelves
    #     relevant_scenes_dict = self.get_relevant_scene_types_from_list(scene_type_list)
    #     relevant_template_name_list = relevant_scenes_dict.values()
    #     filtered_scif = self.scif.loc[self.scif[Const.TEMPLATE_NAME].isin(relevant_template_name_list)]
    #
    #     # Calculate count of display - store_level
    #     display_count_store_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_STORE_LEVEL)
    #     scene_types_in_store = len(filtered_scif[Const.SCENE_FK].unique())
    #     result_store_level = 100 if scene_types_in_store >= store_target else scene_types_in_store / float(store_target)
    #     # self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.pepsico_fk,
    #     #                                numerator_result=scene_types_in_store,
    #     #                                denominator_id=self.store_id, denominator_result=store_target,
    #     #                                identifier_result=display_count_store_level_fk,
    #     #                                result=result_store_level, should_enter=True)
    #
    #     identifier_parent_store_level = self.common.get_dictionary(kpi_fk=display_count_store_level_fk)
    #     self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.pepsico_fk,
    #                                    numerator_result=scene_types_in_store,
    #                                    denominator_id=self.store_id, denominator_result=store_target,
    #                                    identifier_result=identifier_parent_store_level,
    #                                    result=result_store_level, should_enter=True)
    #
    #     # Calculate count of display - category_level
    #     display_count_category_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_CATEGORY_LEVEL)
    #     for category in self.categories_to_calculate:
    #         current_category_target = category_targets[category]
    #         if not current_category_target:
    #             continue
    #         category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
    #         relevant_scenes = [scene_type for scene_type in relevant_template_name_list if
    #                            category.upper() in scene_type.upper()]
    #         filtered_scif_by_cat = filtered_scif.loc[filtered_scif[Const.TEMPLATE_NAME].isin(relevant_scenes)]
    #         if filtered_scif_by_cat.empty:
    #             continue
    #         scene_types_in_cate = len(filtered_scif_by_cat[Const.SCENE_FK].unique())
    #         result_cat_level = 100 if scene_types_in_cate >= current_category_target else scene_types_in_cate / float(
    #             current_category_target)
    #         display_count_category_level_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
    #                                                                              category=category)
    #         # self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.pepsico_fk,
    #         #                                numerator_result=scene_types_in_cate,
    #         #                                denominator_id=category_fk, denominator_result=current_category_target,
    #         #                                identifier_result=display_count_category_level_identifier,
    #         #                                identifier_parent=display_count_category_level_fk,
    #         #                                result=result_cat_level, should_enter=True)
    #         self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.pepsico_fk,
    #                                        numerator_result=scene_types_in_cate,
    #                                        denominator_id=category_fk, denominator_result=current_category_target,
    #                                        identifier_result=display_count_category_level_identifier,
    #                                        identifier_parent=identifier_parent_store_level,
    #                                        result=result_cat_level, should_enter=True)
    #
    #
    #     # Calculate count of display - scene_level
    #     display_count_scene_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_SCENE_LEVEL)
    #     for scene_type in relevant_scenes_dict.keys():
    #         scene_type_target = scene_targets[scene_type]
    #         if not scene_type_target:
    #             continue
    #         actual_scene_name = relevant_scenes_dict[scene_type]
    #         relevant_category = self.get_category_from_template_name(actual_scene_name)
    #         relevant_category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, relevant_category)
    #         scene_type_score = len(
    #             filtered_scif[filtered_scif[Const.TEMPLATE_NAME] == actual_scene_name][Const.SCENE_FK].unique())
    #
    #         result_scene_level = 100 if scene_type_score >= scene_type_target else scene_types_in_store / float(
    #             scene_type_target)
    #         scene_type_fk = self.all_templates.loc[self.all_templates[Const.TEMPLATE_NAME] == actual_scene_name][
    #             Const.TEMPLATE_FK].values[0]
    #         display_count_scene_level_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
    #                                                                           category=relevant_category)
    #         # parent_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
    #         #                                                category=relevant_category)
    #         # self.common.write_to_db_result(fk=display_count_scene_level_fk, numerator_id=self.pepsico_fk,
    #         #                                numerator_result=scene_types_in_store,
    #         #                                denominator_id=relevant_category_fk, denominator_result=scene_type_target,
    #         #                                identifier_result=display_count_scene_level_identifier,
    #         #                                identifier_parent=parent_identifier, context_id=scene_type_fk,
    #         #                                result=result_scene_level, should_enter=True)
    #
    #         self.common.write_to_db_result(fk=display_count_scene_level_fk, numerator_id=self.pepsico_fk,
    #                                        numerator_result=scene_types_in_store,
    #                                        denominator_id=relevant_category_fk, denominator_result=scene_type_target,
    #                                        identifier_result=display_count_scene_level_identifier,
    #                                        identifier_parent=identifier_parent_store_level, context_id=scene_type_fk,
    #                                        result=result_scene_level, should_enter=True)

    def calculate_assortment(self):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        # lvl3_result = self.get_lvl3_assortment_result_main_shelf()
        self.category_assortment_calculation(lvl3_result)
        self.store_assortment_calculation(lvl3_result)

    def get_lvl3_assortment_result_main_shelf(self):
        assortment_result = self.assortment.get_lvl3_relevant_ass()
        if not self.main_shelves and not assortment_result.empty:
            assortment_result.drop(assortment_result.index[0:], inplace=True)
        if assortment_result.empty:
            return assortment_result
        filters = {Const.TEMPLATE_NAME: self.main_shelves}
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        products_in_session = filtered_scif.loc[filtered_scif['facings'] > 0]['product_fk'].values
        assortment_result.loc[assortment_result['product_fk'].isin(products_in_session), 'in_store'] = 1
        return assortment_result

    @log_runtime('Share of shelf pepsicoRU')
    def calculate_share_of_shelf(self):
        """
        The function filters only the relevant scene (type = Main Shelf in category) and calculates the linear SOS and
        the facing SOS for each level (Manufacturer, Category, Sub-Category, Brand).
        The identifier for every kpi will be the current kpi_fk and the relevant attribute according to the level
        E.g sub_category_fk for level 3 or brand_fk for level 4.
        :return:
        """
        # Get all of the KPI fk in advance
        facings_stores_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_MANUFACTURER_SOS)
        facings_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_CATEGORY_SOS)
        facings_sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_SUB_CATEGORY_SOS)
        facings_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_BRAND_SOS)

        linear_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_MANUFACTURER_SOS)
        linear_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_CATEGORY_SOS)
        linear_sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_SUB_CATEGORY_SOS)
        linear_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_BRAND_SOS)

        filter_man_param = {Const.MANUFACTURER_NAME: Const.PEPSICO}
        general_filters = {Const.TEMPLATE_NAME: self.main_shelves}
        facings_level_1_identifier = self.common.get_dictionary(kpi_fk=facings_stores_kpi_fk)
        linear_level_1_identifier = self.common.get_dictionary(kpi_fk=linear_store_kpi_fk)
        num_facings = denom_facings = num_linear = denom_linear = result_facings = result_linear = 0

        if self.main_shelves:
            num_facings, denom_facings, num_linear, denom_linear = self.calculate_sos(
                sos_filters=filter_man_param, **general_filters)
            result_facings = num_facings / float(denom_facings) if denom_facings else 0
            result_linear = num_linear / float(denom_linear) if denom_linear else 0

        # Facings level 1
        self.common.write_to_db_result(fk=facings_stores_kpi_fk, numerator_id=self.pepsico_fk,
                                       identifier_result=facings_level_1_identifier,
                                       numerator_result=num_facings, denominator_id=self.store_id,
                                       denominator_result=denom_facings, result=result_facings,
                                       should_enter=True)
        # Linear level 1
        self.common.write_to_db_result(fk=linear_store_kpi_fk, numerator_id=self.pepsico_fk,
                                       identifier_result=linear_level_1_identifier,
                                       numerator_result=num_linear*100, denominator_id=self.store_id,
                                       denominator_result=denom_linear*100, result=result_linear,
                                       should_enter=True)

        for category in self.categories_to_calculate:
            current_category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
            main_shelves_for_category = self.get_main_shelf_by_category(category)
            if main_shelves_for_category:
                filter_params = {Const.CATEGORY: category, Const.TEMPLATE_NAME: main_shelves_for_category}
                facings_cat_identifier = self.common.get_dictionary(kpi_fk=facings_cat_kpi_fk,
                                                                    category_fk=current_category_fk)
                linear_cat_identifier = self.common.get_dictionary(kpi_fk=linear_cat_kpi_fk,
                                                                   category_fk=current_category_fk)
                num_facings, denom_facings, num_linear, denom_linear = self.calculate_sos(
                    sos_filters=filter_man_param, **filter_params)

                result_facings = num_facings / float(denom_facings) if denom_facings else 0
                result_linear = num_linear / float(denom_linear) if denom_linear else 0

                # Facings level 2
                self.common.write_to_db_result(fk=facings_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                               numerator_result=num_facings, denominator_id=current_category_fk,
                                               denominator_result=denom_facings,
                                               identifier_result=facings_cat_identifier,
                                               identifier_parent=facings_level_1_identifier,
                                               result=result_facings, should_enter = True)
                # Linear level 2
                self.common.write_to_db_result(fk=linear_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                               numerator_result=num_linear*100, denominator_id=current_category_fk,
                                               denominator_result=denom_linear*100,
                                               identifier_result=linear_cat_identifier,
                                               identifier_parent=linear_level_1_identifier,
                                               result=result_linear, should_enter = True)

                for sub_cat in self.get_relevant_sub_categories_for_category(category):
                    current_sub_category_fk = self.get_relevant_pk_by_name(Const.SUB_CATEGORY, sub_cat)
                    filter_sub_cat_param = {Const.SUB_CATEGORY: sub_cat, Const.CATEGORY: category,
                                            Const.TEMPLATE_NAME: main_shelves_for_category}
                    facings_sub_cat_identifier = self.common.get_dictionary(kpi_fk=facings_sub_cat_kpi_fk,
                                                                            sub_category_fk=current_sub_category_fk)
                    linear_sub_cat_identifier = self.common.get_dictionary(kpi_fk=linear_sub_cat_kpi_fk,
                                                                           sub_category_fk=current_sub_category_fk)
                    num_facings, denom_facings, num_linear, denom_linear = self.calculate_sos(
                        sos_filters=filter_man_param, **filter_sub_cat_param)

                    if denom_facings and denom_linear:
                        # Facings level 3
                        self.common.write_to_db_result(fk=facings_sub_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                                       numerator_result=num_facings, denominator_id=current_sub_category_fk,
                                                       denominator_result=denom_facings,
                                                       identifier_result=facings_sub_cat_identifier,
                                                       identifier_parent=facings_cat_identifier,
                                                       result=num_facings / float(denom_facings), should_enter = True)
                        # Linear level 3
                        self.common.write_to_db_result(fk=linear_sub_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                                       numerator_result=num_linear*100, denominator_id=current_sub_category_fk,
                                                       denominator_result=denom_linear*100,
                                                       identifier_result=linear_sub_cat_identifier,
                                                       identifier_parent=linear_cat_identifier,
                                                       result=num_linear / float(denom_linear), should_enter = True)

                        for brand_name in self.get_relevant_brands_for_sub_category(sub_cat):
                            current_brand_fk = self.get_relevant_pk_by_name(Const.BRAND, brand_name)
                            filter_sos_brand = {Const.BRAND_NAME: brand_name, Const.SUB_CATEGORY: sub_cat,
                                                Const.MANUFACTURER_NAME: Const.PEPSICO}
                            filter_general_brand_param = {Const.SUB_CATEGORY: sub_cat, Const.CATEGORY: category,
                                                          Const.TEMPLATE_NAME: main_shelves_for_category}
                            facings_brand_identifier = self.common.get_dictionary(kpi_fk=facings_brand_kpi_fk,
                                                                                  brand_fk=current_brand_fk)
                            linear_brand_identifier = self.common.get_dictionary(kpi_fk=linear_brand_kpi_fk,
                                                                                 brand_fk=current_brand_fk)
                            num_facings, denom_facings, num_linear, denom_linear = self.calculate_sos(
                                sos_filters=filter_sos_brand, **filter_general_brand_param)

                            if denom_facings and denom_linear:
                                # Facings level 4
                                self.common.write_to_db_result(fk=facings_brand_kpi_fk, numerator_id=current_brand_fk,
                                                               numerator_result=num_facings, denominator_id=current_sub_category_fk,
                                                               denominator_result=denom_facings,
                                                               identifier_result=facings_brand_identifier,
                                                               identifier_parent=facings_sub_cat_identifier,
                                                               result=num_facings / float(denom_facings), should_enter=True)
                                # Linear level 4
                                self.common.write_to_db_result(fk=linear_brand_kpi_fk, numerator_id=current_brand_fk,
                                                               numerator_result=num_linear*100, denominator_id=current_sub_category_fk,
                                                               denominator_result=denom_linear*100,
                                                               identifier_result=linear_brand_identifier,
                                                               identifier_parent=linear_sub_cat_identifier,
                                                               result=num_linear / float(denom_linear), should_enter=True)

    # Utils functions with a slight change from the SDK factory:
    def calculate_sos(self, sos_filters, include_empty=Const.EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The numerator facings, denominator facings, numerator linear and denominator linear.
        """
        if include_empty == Const.EXCLUDE_EMPTY:
            general_filters[Const.PRODUCT_TYPE] = (Const.EMPTY, Const.EXCLUDE_FILTER)
        numerator_facings, numerator_linear = self.calculate_share_space(**dict(sos_filters, **general_filters))
        denominator_facings, denominator_linear = self.calculate_share_space(**general_filters)
        return numerator_facings, denominator_facings, numerator_linear / 1000.0, denominator_linear / 1000.0

    def calculate_share_space(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of facings and the shelf width (in mm) according to the filters.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        sum_of_facings = filtered_scif['facings'].sum()
        space_length = filtered_scif['net_len_ign_stack'].sum()
        return sum_of_facings, space_length

    def category_assortment_calculation(self, lvl3_result):
        """
        This function calculates 3 levels of assortment :
        level3 is assortment SKU
        level2 is assortment groups
        """
        osa_product_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.OSA_SKU_LEVEL)
        oos_product_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.OOS_SKU_LEVEL)
        osa_category_level_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.OSA_CATEGORY_LEVEL)
        oos_category_level_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.OOS_CATEGORY_LEVEL)

        if not lvl3_result.empty:
            cat_df = self.all_products[['product_fk', 'category_fk']]
            lvl3_with_cat = lvl3_result.merge(cat_df, on='product_fk', how='left')
            lvl3_with_cat = lvl3_with_cat[lvl3_with_cat['category_fk'].notnull()]

            for result in lvl3_with_cat.itertuples():
                if result.in_store == 1:
                    score = Const.DISTRIBUTION
                else:
                    score = Const.OOS
                # Distribution
                self.common_v1.write_to_db_result_new_tables(fk=osa_product_level_fk, numerator_id=result.product_fk,
                                                             numerator_result=score,
                                                             result=score, denominator_id=result.category_fk,
                                                             denominator_result=1, score=score,
                                                             score_after_actions=score)
                if score == Const.OOS:
                    # OOS
                    self.common_v1.write_to_db_result_new_tables(oos_product_level_fk,
                                                                 numerator_id=result.product_fk, numerator_result=score,
                                                                 result=score, denominator_id=result.category_fk,
                                                                 denominator_result=1, score=score,
                                                                 score_after_actions=score)
            category_fk_list = lvl3_with_cat['category_fk'].unique()
            for cat in category_fk_list:
                lvl3_result_cat = lvl3_with_cat[lvl3_with_cat["category_fk"] == cat]
                lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result_cat)
                for result in lvl2_result.itertuples():
                    denominator_res = result.total
                    res = np.divide(float(result.passes), float(denominator_res))
                    # Distribution
                    self.common_v1.write_to_db_result_new_tables(fk=osa_category_level_kpi_fk,
                                                                 numerator_id=self.pepsico_fk,
                                                                 numerator_result=result.passes,
                                                                 denominator_id=cat,
                                                                 denominator_result=denominator_res,
                                                                 result=res, score=res, score_after_actions=res)

                    # OOS
                    self.common_v1.write_to_db_result_new_tables(fk=oos_category_level_kpi_fk,
                                                                 numerator_id=self.pepsico_fk,
                                                                 numerator_result=denominator_res - result.passes,
                                                                 denominator_id=cat,
                                                                 denominator_result=denominator_res,
                                                                 result=1 - res, score=(1 - res),
                                                                 score_after_actions=1 - res)
                self.assortment.LVL2_HEADERS.extend(['passes', 'total'])
        return

    def store_assortment_calculation(self, lvl3_result):
        """
        This function calculates the KPI results.
        """
        dist_store_level_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.OSA_STORE_LEVEL)
        oos_store_level_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.OOS_STORE_LEVEL)
        # for result in lvl3_result.itertuples():
        #     if result.in_store == 1:
        #         score = Const.DISTRIBUTION
        #     else:
        #         score = Const.OOS
        #     # Distribution
        #     self.common_v1.write_to_db_result_new_tables(fk=?????, numerator_id=result.product_fk,
        #                                                 numerator_result=score,
        #                                                 result=score, denominator_id=self.store_id,
        #                                                 denominator_result=1, score=score)
        #     if score == Const.OOS:
        #         # OOS
        #         self.common_v1.write_to_db_result_new_tables(fk=?????, numerator_id=result.product_fk,
        #                                                     numerator_result=score,
        #                                                     result=score, denominator_id=self.store_id,
        #                                                     denominator_result=1, score=score,
        #                                                     score_after_actions=score)

        if not lvl3_result.empty:
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if not pd.isnull(result.target) and not pd.isnull(result.group_target_date) and result.group_target_date <= self.visit_date:
                    denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res))
                # Distribution
                self.common_v1.write_to_db_result_new_tables(fk=dist_store_level_kpi_fk, numerator_id=self.pepsico_fk,
                                                             denominator_id=self.store_id,
                                                             numerator_result=result.passes,
                                                             denominator_result=denominator_res,
                                                             result=res, score=res, score_after_actions=res)

                # OOS
                self.common_v1.write_to_db_result_new_tables(fk=oos_store_level_kpi_fk,
                                                             numerator_id=self.pepsico_fk,
                                                             numerator_result=denominator_res - result.passes,
                                                             denominator_id=self.store_id,
                                                             denominator_result=denominator_res,
                                                             result=1 - res, score=1 - res, score_after_actions=1 - res)
        return
