import os
import pandas as pd
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Projects.PEPSICORU_SAND.Utils.Const import Const
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
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
        self.sos_results = {}

        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.categories_to_calculate = self.get_relevant_categories_for_session()
        self.sos = SOS(data_provider, output, self.rds_conn)
        self.assortment = Assortment(self.data_provider, self.output, common=self.common)
        self.toolbox = GENERALToolBox(data_provider)

    @staticmethod
    def get_scene_type_by_category(current_category):
        """
        This function gets a category and return the relevant scene type for the SOS
        :param current_category: One of the product's categories. E.g: Snacks.
        :return: The relevant scene type to the current category
        """
        if current_category == Const.SNACKS:
            return Const.MAIN_SHELF_SNACKS
        elif current_category == Const.BEVERAGES:
            return Const.MAIN_SHELF_BEVERAGES
        else:
            return Const.MAIN_SHELF_JUICES

    def get_scene_type_by_sub_cat(self, sub_cat):
        """
        This function gets a sub_category and return the relevant scene type for the SOS
        :param sub_cat: One of the product's sub_categories. E.g: TODO todo todo.
        :return: The relevant scene type to the current sub_category
        """
        current_category = self.scif.loc[self.scif[Const.SUB_CATEGORY] == sub_cat][Const.CATEGORY].values[0]
        return self.get_scene_type_by_category(current_category)

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

    def get_relevant_pk_by_name(self, filter_by, filter_param):
        """
        This function gets a filter name and returns the relevant pk
        :param filter_by: filter by name E.g: 'category', 'brand'.
        :param filter_param: The param to filter by. E.g: if filter_by = 'category', filter_param could be 'Snack'
        :return: The relevant pk
        """
        pk_field = filter_by + Const.FK
        field_name = filter_by + Const.NAME
        if filter_by == Const.CATEGORY:
            return self.scif.loc[self.scif[Const.CATEGORY] == filter_param][pk_field].values[0]
        else:
            return self.scif.loc[self.scif[field_name] == filter_param][pk_field].values[0]


    @log_runtime('Share of shelf pepsicoRU')
    def share_of_shelf_calculator(self):
        """
        The function filters only the relevant scene (type = Main Shelf in category) and calculates the linear SOS and
        the facing SOS for each level (Manufacturer, Category, Sub-Category, Brand).
        :return:
        """
        filter_manu_param = {Const.MANUFACTURER_NAME: Const.PEPSICO}
        pepsiCo_fk = self.get_relevant_pk_by_name(Const.MANUFACTURER, Const.PEPSICO)

        for category in self.categories_to_calculate:
            filter_params = {Const.CATEGORY: category, Const.TEMPLATE_NAME: self.get_scene_type_by_category(category)}
            # Calculate Facings SOS
            numerator_score, denominator_score, result = self.calculate_facings_sos(filter_manu_param, filter_params)
            facings_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_CATEGORY)
            current_category_fk = self.get_relevant_pk_by_name(Const.CATEGORY, category)
            self.common.write_to_db_result(fk=facings_cat_kpi_fk, numerator_id=pepsiCo_fk,
                                           numerator_result=numerator_score, denominator_id=current_category_fk,
                                           denominator_result=denominator_score, result=result, score=result)
            # Calculate Linear SOS
            numerator_score, denominator_score, result = self.calculate_linear_sos(filter_manu_param, filter_params)
            linear_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_CATEGORY)
            self.common.write_to_db_result(fk=linear_cat_kpi_fk, numerator_id=pepsiCo_fk,
                                           numerator_result=numerator_score, denominator_id=current_category_fk,
                                           denominator_result=denominator_score, result=result, score=result)

        for sub_cat in self.scif[Const.SUB_CATEGORY].unique().tolist():
            filter_sub_cat_param = {Const.SUB_CATEGORY: sub_cat,
                                    Const.TEMPLATE_NAME: self.get_scene_type_by_sub_cat(sub_cat)}
            # Calculate Facings SOS
            sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_SUB_CATEGORY)
            numerator_score, denominator_score, result = self.calculate_facings_sos(filter_manu_param,
                                                                                    filter_sub_cat_param)
            current_sub_category_fk = self.get_relevant_pk_by_name(Const.SUB_CATEGORY, sub_cat)
            self.common.write_to_db_result(fk=sub_cat_kpi_fk, numerator_id=pepsiCo_fk,
                                           numerator_result=numerator_score, denominator_id=current_sub_category_fk,
                                           denominator_result=denominator_score, result=result, score=result)
            # Calculate Linear SOS
            numerator_score, denominator_score, result = self.calculate_linear_sos(filter_manu_param,
                                                                                   filter_sub_cat_param)
            linear_sub_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_SUB_CATEGORY)
            self.common.write_to_db_result(fk=linear_sub_cat_kpi_fk, numerator_id=pepsiCo_fk,
                                           numerator_result=numerator_score, denominator_id=current_sub_category_fk,
                                           denominator_result=denominator_score, result=result, score=result)

        for brand in self.scif[Const.BRAND_NAME]:
            filter_brand_param = {Const.BRAND_NAME: brand}
            # Calculate Facings SOS
            facings_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.FACINGS_BRAND)


            # Calculate Linear SOS
            linear_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.LINEAR_BRAND)

        return

    def get_relevant_session_categories(self):
        """
        This functions returns the relevant categories to calculate according to the current scene types
        :return: list of the categories that needs to be calculate
        """
        # todo
        # Filter scif???
        return [Const.SNACKS]

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        session_categories = self.get_relevant_session_categories()
        score = 0
        return score


###################################### Plaster ######################################


    def calculate_share_space_length(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        filtered_matches = \
            self.match_product_in_scene[self.toolbox.get_filter_condition(self.match_product_in_scene, **filters)]
        space_length = filtered_matches['width_mm_advance'].sum()
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
            return 0, 0
        else:
            return numerator_width, denominator_width, (numerator_width / float(denominator_width))

    def calculate_share_facings(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total number of the relevant facings occupy.
        """
        filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        sum_of_facings = filtered_scif['width_mm_advance'].sum()
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
            return 0, 0
        else:
            return numerator_counter, denominator_counter, (numerator_counter / float(denominator_counter))
