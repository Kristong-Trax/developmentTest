import os
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
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
        self.commonv1 = CommonV1(self.data_provider)
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.pepsico_fk = self.get_relevant_pk_by_name(Const.MANUFACTURER, Const.PEPSICO)
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.categories_to_calculate = self.get_relevant_categories_for_session()
        self.toolbox = GENERALToolBox(data_provider)
        self.main_shelves = [scene_type for scene_type in self.scif[Const.TEMPLATE_NAME].unique().tolist() if
                             Const.MAIN_SHELF in scene_type.upper()]

    def get_main_shelf_by_category(self, current_category):
        """
        This function gets a category and return the relevant scene type for the SOS
        :param current_category: One of the product's categories. E.g: Snacks.
        :return: The relevant scene type to the current category
        """
        for main_shelf in self.main_shelves:
            if current_category in main_shelf.upper():
                return main_shelf

    @staticmethod
    def get_category_from_template_name(template_name):
        """
        This function gets a template name (scene_type) and return it's relevant category.
        :param template_name: The scene type.
        :return: category name
        """
        if Const.SNACKS in template_name:
            return Const.SNACKS
        elif Const.BEVERAGES in template_name:
            return Const.BEVERAGES
        elif Const.JUICES in template_name:
            return Const.JUICES
        else:
            Log.warning("Couldn't find a matching category for template name = {}".format(template_name))
            return None

    def get_scene_type_by_sub_cat(self, sub_cat):
        """
        This function gets a sub_category and return the relevant scene type for the SOS
        :param sub_cat: One of the product's sub_categories. E.g: TODO todo todo.
        :return: The relevant scene type to the current sub_category
        """
        current_category = self.scif.loc[self.scif[Const.SUB_CATEGORY] == sub_cat][Const.CATEGORY].values[0]
        return self.get_main_shelf_by_category(current_category)

    def get_relevant_categories_for_session(self):
        """
        This function returns a list of the relevant categories according to the scene_types in the session
        :return: List of the relevant categories
        """
        relevant_categories = set()
        scene_types = self.scif[Const.TEMPLATE_NAME].unique().tolist()
        for scene_type in scene_types:
            if Const.SNACKS in scene_type.upper():
                relevant_categories.add(Const.SNACKS)
            elif Const.BEVERAGES in scene_type.upper():
                relevant_categories.add(Const.BEVERAGES)
            else:
                relevant_categories.add(Const.JUICES)
        return list(relevant_categories)

    def get_relevant_sub_categories_for_session(self):
        """
        This function returns a list of the relevant categories according to the scene_types in the session
        :return: List of the relevant categories
        """
        sub_categories = self.scif[Const.SUB_CATEGORY].unique().tolist()
        if None in sub_categories:
            sub_categories.remove(None)
        for sub_cat in sub_categories:
            relevant_category = self.get_unique_attribute_from_filters(Const.SUB_CATEGORY, sub_cat, Const.CATEGORY)
            if not relevant_category:
                sub_categories.remove(sub_cat)
        return sub_categories

    def get_relevant_attributes_for_sos(self, attribute):
        """ # todo todo todo todo todo ?
        This function returns a list of the relevant attributes according to the scene_types in the session.
        Firstly we filter by main shelf and than check all of the possible attributes.
        :param attribute: The attribute you would like to get. E.g: brand_name, category etc.
        :return: List of the relevant categories
        """
        filtered_scif = self.scif[self.scif[Const.TEMPLATE_NAME].isin(self.main_shelves)]
        list_of_attribute = filtered_scif[attribute].unique().tolist()
        for attr in list_of_attribute:
            if filtered_scif[filtered_scif[attribute] == attr].empty:
                list_of_attribute.remove(attr)
        return list_of_attribute

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

    def get_unique_attribute_from_filters(self, filter_by, param, attribute_to_get):
        """
        This function gets an attribute name and a parameter and return the attribute the user wants to get.
        For example: The function can get filter_by=sub_category_name and param=snacks and
        attribute_to_get=sub_category_fk and it will return 41 (SNACK sub_cat fk).
        :param filter_by: The relevant attribute E.g: brand_name, sub_category_fk
        :param param: The parameter that fits the attribute: E.g snacks, adrenaline.
        :param attribute_to_get: Which attribute to return! E.g: brand_fk, category_fk etc.
        :return: The category fk that match the filter params.
        """
        unique_attr = self.products[self.products[filter_by] == param][attribute_to_get].unique()
        if len(unique_attr) > 1:
            Log.warning("Several {} match to the following {}: {}".format(attribute_to_get, filter_by, param))
        return unique_attr[0]

    @log_runtime('Share of shelf pepsicoRU')
    def calculate_share_of_shelf(self):
        """
        The function filters only the relevant scene (type = Main Shelf in category) and calculates the linear SOS and
        the facing SOS for each level (Manufacturer, Category, Sub-Category, Brand).
        The identifier for every kpi will be the current kpi_fk and the relevant attribute according to the level
        E.g sub_category_fk for level 3 or brand_fk for level 4.
        :return:
        """
        # Level 1
        filter_manu_param = {Const.MANUFACTURER_NAME: Const.PEPSICO}
        general_filters = {Const.TEMPLATE_NAME: self.main_shelves}
        # Calculate Facings SOS
        numerator_score, denominator_score, result = self.calculate_facings_sos(sos_filters=filter_manu_param,
                                                                                **general_filters)
        facings_stores_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_MANUFACTURER_SOS)
        facings_level_1_identifier = self.common.get_dictionary(kpi_fk=facings_stores_kpi_fk)
        self.common.write_to_db_result(fk=facings_stores_kpi_fk, numerator_id=self.pepsico_fk,
                                       identifier_result=facings_level_1_identifier,
                                       numerator_result=numerator_score, denominator_id=self.store_id,
                                       denominator_result=denominator_score, result=result, score=result)
        # Calculate Linear SOS
        linear_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_MANUFACTURER_SOS)
        linear_level_1_identifier = self.common.get_dictionary(kpi_fk=linear_store_kpi_fk)
        numerator_score, denominator_score, result = self.calculate_linear_sos(sos_filters=filter_manu_param,
                                                                               **general_filters)
        self.common.write_to_db_result(fk=linear_store_kpi_fk, numerator_id=self.pepsico_fk,
                                       identifier_result=linear_level_1_identifier,
                                       numerator_result=numerator_score, denominator_id=self.store_id,
                                       denominator_result=denominator_score, result=result, score=result)
        # Level 2
        for category in self.categories_to_calculate:
            filter_params = {Const.CATEGORY: category, Const.TEMPLATE_NAME: self.get_main_shelf_by_category(category)}
            current_category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
            # Calculate Facings SOS
            facings_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_CATEGORY_SOS)
            numerator_score, denominator_score, result = self.calculate_facings_sos(sos_filters=filter_manu_param,
                                                                                    **filter_params)
            level_2_facings_cat_identifier = self.common.get_dictionary(kpi_fk=facings_cat_kpi_fk,
                                                                        category_fk=current_category_fk)
            self.common.write_to_db_result(fk=facings_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                           numerator_result=numerator_score, denominator_id=current_category_fk,
                                           denominator_result=denominator_score,
                                           identifier_result=level_2_facings_cat_identifier,
                                           identifier_parent=facings_level_1_identifier, result=result, score=result)
            # Calculate Linear SOS
            linear_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_CATEGORY_SOS)
            numerator_score, denominator_score, result = self.calculate_linear_sos(sos_filters=filter_manu_param,
                                                                                   **filter_params)
            level_2_linear_cat_identifier = self.common.get_dictionary(kpi_fk=linear_cat_kpi_fk,
                                                                       category_fk=current_category_fk)
            self.common.write_to_db_result(fk=linear_cat_kpi_fk, numerator_id=self.pepsico_fk,
                                           numerator_result=numerator_score, denominator_id=current_category_fk,
                                           denominator_result=denominator_score,
                                           identifier_result=level_2_linear_cat_identifier,
                                           identifier_parent=linear_level_1_identifier, result=result, score=result)
        # Level 3
        for sub_cat in self.get_relevant_sub_categories_for_session():
            curr_category_fk = self.get_unique_attribute_from_filters(Const.SUB_CATEGORY, sub_cat, Const.CATEGORY_FK)
            current_sub_category_fk = self.get_relevant_pk_by_name(Const.SUB_CATEGORY, sub_cat)
            filter_sub_cat_param = {Const.SUB_CATEGORY: sub_cat,
                                    Const.TEMPLATE_NAME: self.get_scene_type_by_sub_cat(sub_cat)}
            # Calculate Facings SOS
            facings_sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_SUB_CATEGORY_SOS)
            numerator_score, denominator_score, result = self.calculate_facings_sos(sos_filters=filter_manu_param,
                                                                                    **filter_sub_cat_param)
            level_3_facings_sub_cat_identifier = self.common.get_dictionary(kpi_fk=facings_sub_cat_kpi_fk,
                                                                            sub_category_fk=current_sub_category_fk)
            parent_identifier = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_CATEGORY_SOS), category_fk=curr_category_fk)
            self.common.write_to_db_result(fk=facings_sub_cat_kpi_fk, numerator_id=current_sub_category_fk,
                                           numerator_result=numerator_score, denominator_id=current_sub_category_fk,
                                           identifier_result=level_3_facings_sub_cat_identifier,
                                           identifier_parent=parent_identifier,
                                           denominator_result=denominator_score, result=result, score=result)
            # Calculate Linear SOS
            numerator_score, denominator_score, result = self.calculate_linear_sos(sos_filters=filter_manu_param,
                                                                                   **filter_sub_cat_param)
            linear_sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_SUB_CATEGORY_SOS)
            level_3_linear_sub_cat_identifier = self.common.get_dictionary(kpi_fk=linear_sub_cat_kpi_fk,
                                                                           sub_category_fk=current_sub_category_fk)
            parent_identifier = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_CATEGORY_SOS), category_fk=curr_category_fk)
            self.common.write_to_db_result(fk=linear_sub_cat_kpi_fk, numerator_id=current_sub_category_fk,
                                           numerator_result=numerator_score, denominator_id=current_sub_category_fk,
                                           identifier_result=level_3_linear_sub_cat_identifier,
                                           identifier_parent=parent_identifier,
                                           denominator_result=denominator_score, result=result, score=result)
        # Level 4
        brands_list = self.scif[self.scif[Const.MANUFACTURER_NAME] == Const.PEPSICO][Const.BRAND_NAME].unique().tolist()
        if None in brands_list:
            brands_list.remove(None)
        for brand in brands_list:
            relevant_category = self.get_unique_attribute_from_filters(Const.BRAND_NAME, brand, Const.CATEGORY)
            relevant_sub_cat_fk = self.get_unique_attribute_from_filters(Const.BRAND_NAME, brand, Const.SUB_CATEGORY_FK)
            filter_brand_param = {Const.BRAND_NAME: brand, Const.MANUFACTURER_NAME: Const.PEPSICO}
            general_filters = {Const.CATEGORY: relevant_category,
                               Const.TEMPLATE_NAME: self.get_main_shelf_by_category(relevant_category)}
            current_brand_fk = self.get_relevant_pk_by_name(Const.BRAND, brand)
            # Calculate Facings SOS
            facings_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_BRAND_SOS)
            numerator_score, denominator_score, result = self.calculate_facings_sos(sos_filters=filter_brand_param,
                                                                                    **general_filters)
            level_4_facings_brand_identifier = self.common.get_dictionary(kpi_fk=facings_brand_kpi_fk,
                                                                          brand_fk=current_brand_fk)
            parent_identifier = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_SUB_CATEGORY_SOS), sub_cat=relevant_sub_cat_fk)
            self.common.write_to_db_result(fk=facings_brand_kpi_fk, numerator_id=current_brand_fk,
                                           numerator_result=numerator_score, denominator_id=relevant_sub_cat_fk,
                                           identifier_result=level_4_facings_brand_identifier,
                                           identifier_parent=parent_identifier,
                                           denominator_result=denominator_score, result=result, score=result)
            # Calculate Linear SOS
            linear_brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_BRAND_SOS)
            numerator_score, denominator_score, result = self.calculate_facings_sos(filter_brand_param,
                                                                                    **general_filters)
            level_4_linear_brand_identifier = self.common.get_dictionary(kpi_fk=linear_brand_kpi_fk,
                                                                         brand_fk=current_brand_fk)
            parent_identifier = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_SUB_CATEGORY_SOS), sub_cat=relevant_sub_cat_fk)
            self.common.write_to_db_result(fk=linear_brand_kpi_fk, numerator_id=current_brand_fk,
                                           numerator_result=numerator_score, denominator_id=relevant_sub_cat_fk,
                                           identifier_result=level_4_linear_brand_identifier,
                                           identifier_parent=parent_identifier,
                                           denominator_result=denominator_score, result=result, score=result)
            return

    def calculate_count_of_display(self):
        """
        This function will calculate the Count of # of Pepsi Displays KPI
        :return:
        """
        # TODO: TARGETS TARGETS TARGETS
        # Filtering out the main shelves
        filtered_scif = self.scif.loc[~self.scif[Const.TEMPLATE_NAME].isin(self.main_shelves)]
        if filtered_scif.empty:
            return

        # Calculate count of display - store_level
        display_count_store_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_STORE_LEVEL)
        scene_types_in_store = len(filtered_scif[Const.SCENE_FK].unique())
        self.common.write_to_db_result(fk=display_count_store_level_fk, numerator_id=self.store_id,
                                       numerator_result=scene_types_in_store,
                                       identifier_result=display_count_store_level_fk,
                                       result=scene_types_in_store, score=0)

        # Calculate count of display - category_level
        display_count_category_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_CATEGORY_LEVEL)
        for category in self.categories_to_calculate:
            category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
            relevant_scenes = [scene_type for scene_type in filtered_scif[Const.TEMPLATE_NAME].unique().tolist() if
                               category in scene_type.upper()]
            filtered_scif_by_cat = filtered_scif.loc[filtered_scif[Const.TEMPLATE_NAME].isin(relevant_scenes)]
            if filtered_scif_by_cat.empty:
                continue
            scene_types_in_category = len(filtered_scif_by_cat[Const.SCENE_FK].unique())
            display_count_category_level_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
                                                                                 category=category)
            self.common.write_to_db_result(fk=display_count_category_level_fk, numerator_id=category_fk,
                                           numerator_result=scene_types_in_category,
                                           identifier_result=display_count_category_level_identifier,
                                           identifier_parent=display_count_category_level_fk,
                                           result=scene_types_in_category, score=scene_types_in_category)

        # Calculate count of display - scene_level
        display_count_scene_level_fk = self.common.get_kpi_fk_by_kpi_type(Const.DISPLAY_COUNT_SCENE_LEVEL)
        for scene_type in filtered_scif[Const.TEMPLATE_NAME].unique().tolist():
            relevant_category = self.get_category_from_template_name(scene_type)
            scene_type_score = len(
                filtered_scif[filtered_scif[Const.TEMPLATE_NAME] == scene_type][Const.SCENE_FK].unique())
            scene_type_fk = self.get_relevant_pk_by_name(Const.TEMPLATE, scene_type)
            display_count_scene_level_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
                                                                              category=relevant_category)
            parent_identifier = self.common.get_dictionary(kpi_fk=display_count_category_level_fk,
                                                           category=relevant_category)
            self.common.write_to_db_result(fk=display_count_scene_level_fk, numerator_id=scene_type_fk,
                                           numerator_result=scene_type_score,
                                           identifier_result=display_count_scene_level_identifier,
                                           identifier_parent=parent_identifier,
                                           result=scene_type_score, score=0)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_share_of_shelf()
        self.calculate_count_of_display()
        Assortment(self.data_provider, self.output, common=self.commonv1).main_assortment_calculation()

    ###################################### Plaster ######################################

    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = \
            self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        space_length = filtered_matches['net_len_add_stack'].sum()
        return space_length

    def calculate_linear_sos(self, sos_filters, include_empty=Const.EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The numerator, denominator and ratio score.
        """
        if include_empty == Const.EXCLUDE_EMPTY:
            general_filters['product_type'] = (Const.EMPTY, Const.EXCLUDE_FILTER)

        numerator_width = self.calculate_share_space_length(**dict(sos_filters, **general_filters))
        denominator_width = self.calculate_share_space_length(**general_filters)

        if denominator_width == 0:
            return 0, 0, 0
        else:
            return numerator_width / 1000, denominator_width / 1000, (numerator_width / float(denominator_width))

    def calculate_share_facings(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of the relevant facings occupy.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        sum_of_facings = filtered_scif['facings'].sum()
        return sum_of_facings

    def calculate_facings_sos(self, sos_filters, include_empty=Const.EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general data frame is filtered by.
        :return: The numerator, denominator and ratio score.
        """
        if include_empty == Const.EXCLUDE_EMPTY:
            general_filters['product_type'] = (Const.EMPTY, Const.EXCLUDE_FILTER)
        numerator_counter = self.calculate_share_facings(**dict(sos_filters, **general_filters))
        denominator_counter = self.calculate_share_facings(**general_filters)
        if denominator_counter == 0:
            return 0, 0, 0
        else:
            return numerator_counter, denominator_counter, (numerator_counter / float(denominator_counter))
