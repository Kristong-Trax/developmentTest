
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils.ParseTemplates import parse_template
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.CBCDAIRYIL.Utils.Consts import Consts
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
from KPIUtils_v2.Calculations.EyeLevelCalculations import calculate_eye_level

__author__ = 'idanr'


class CBCDAIRYILToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.common = Common(self.data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_uid = self.data_provider.session_uid
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'][0]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpis_data = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_SHEET, lower_headers_row_index=1)
        self.kpi_weights = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_WEIGHT, lower_headers_row_index=0)
        self.gap_data = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_GAP, lower_headers_row_index=0)
        self.template_data = self.filter_template_data()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]

        self.sos = SOS(self.data_provider)
        self.survey = Survey(self.data_provider)
        self.block = Block(self.data_provider)
        self.availability = Availability(self.data_provider)
        self.general_toolbox = GENERALToolBox(self.data_provider)

        # self.match_display_in_scene = self.get_match_display()
        # self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        # self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        # self.tools = CBCILCBCIL_GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        # self.kpi_static_data = self.get_kpi_static_data()
        # self.kpi_results_queries = []
        # self.gaps = pd.DataFrame(columns=[self.KPI_NAME, self.KPI_ATOMIC_NAME, self.GAPS])
        # self.gaps_queries = []
        # self.cbcil_id = self.get_own_manufacturer_pk()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        At first it fetches the relevant Sets (according to the stores attributes) and go over all of the relevant
        Atomic KPIs based on the project's template.
        Than, It aggregates the result per KPI using the weights and at last aggregates for the set level.
        """
        if self.template_data.empty:
            Log.warning("There isn't relevant data in the template for store fk = {}! Exiting...".format(self.store_id))
            return
        kpi_set, kpis_list = self.get_relevant_kpis_for_calculation()
        total_set_scores = list()
        for kpi_name in kpis_list:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
            atomics_df = self.get_atomics_to_calculate(kpi_name)
            atomic_results = self.calculate_atomic_results(kpi_fk, atomics_df)  # Atomic level
            kpi_results = self.calculate_kpi_result(kpi_name, atomic_results)   # KPI level
            total_set_scores.append(self.get_kpi_details(kpi_set, kpi_fk, kpi_name, kpi_results))

    def calculate_atomic_results(self, kpi_fk, atomics_df):
        """
        This method calculates the result for every atomic KPI (the lowest level) that are relevant for the kpi_fk.
        :param kpi_fk: The KPI FK that the atomic "belongs" too.
        :param atomics_df: The relevant Atomic KPIs from the project's template.
        :return: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        """
        # identifier_result_kpi = self.get_identifier_result_kpi_by_name(kpi)
        scores = list()
        for i in atomics_df.index:
            current_atomic = atomics_df.iloc[i]
            kpi_type = current_atomic.get(Consts.KPI_TYPE)  # TODO: CHECK FOR SINGLE ATOMIC
            general_filters = self.get_general_filters(current_atomic)
            atomic_weight = float(current_atomic.get(Consts.WEIGHT)) if current_atomic.get(Consts.WEIGHT) else None
            if not self.validate_atomic_kpi(**general_filters):
                continue
            elif kpi_type in [Consts.AVAILABILITY]:
                atomic_score = self.calculate_availability(**general_filters)
            elif kpi_type == Consts.AVAILABILITY_FROM_BOTTOM:
                atomic_score = self.calculate_availability_from_bottom(**general_filters)
            elif kpi_type == Consts.MIN_2_AVAILABILITY:
                atomic_score = self.calculate_min_2_availability(**general_filters)
            elif kpi_type == Consts.SURVEY:
                atomic_score = self.calculate_survey(**general_filters)
            elif kpi_type == Consts.BRAND_BLOCK:
                atomic_score = self.calculate_brand_block(**general_filters)
            elif kpi_type == Consts.EYE_LEVEL:
                atomic_score = self.calculate_eye_level(**general_filters)
            else:
                Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                continue

            if atomic_score == 0:
                self.add_gap(current_atomic)
            scores.append((atomic_score, atomic_weight))

            atomic_fk_lvl_2 = self.common.get_kpi_fk_by_kpi_type(current_atomic[Consts.KPI_ATOMIC_NAME])
            self.common.write_to_db_result(fk=atomic_fk_lvl_2, numerator_id=Consts.CBCIL_MANUFACTURER,
                                           denominator_id=self.store_id,
                                           identifier_parent=kpi_fk,
                                           result=atomic_score, score=atomic_score, should_enter=True)
        return scores

    def get_relevant_kpis_for_calculation(self):
        """
        This function retrieve the relevant KPIs to calculate from the template
        :return: A tuple: (set_name, [kpi1, kpi2, kpi3...]) to calculate.
        """
        kpi_set = self.template_data[Consts.KPI_SET].values[0]
        kpis = self.template_data[self.template_data[Consts.KPI_SET].str.encode('utf-8') ==
                                  kpi_set.encode('utf-8')][Consts.KPI_NAME].unique().tolist()
        return kpi_set, kpis

    def get_atomics_to_calculate(self, kpi_name):
        """
        This method filters the KPIs data to the relevant atomics in order to calculate the relevant atomic level
        for the current KPI.
        :param kpi_name: The hebrew KPI name from the template.
        :return: A DataFrame that contains data about the relevant Atomic KPIs.
        """
        atomics = self.template_data[
            self.template_data[Consts.KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')]
        return atomics

    def get_store_attributes(self, attributes_names):
        """
        This function encodes and returns the relevant store attribute.
        :param attributes_names: List of requeted store attributes to return.
        :return: A dictionary relevant attribute after encoding (if necessary).
        """
        # Filter store attributes
        store_info_dict = self.store_info.iloc[0].to_dict()
        filtered_store_info = {store_att: store_info_dict[store_att] for store_att in attributes_names}
        # Encode the attributes if necessary
        for attr in attributes_names:
            if isinstance(filtered_store_info[attr], (unicode, str)):
                filtered_store_info[attr] = [filtered_store_info[attr].encode('utf-8')]
        return filtered_store_info

    def filter_template_data(self):
        """
        This function responsible to filter the relevant template data..
        :return: A DataFrame with filtered Data by store attributes.
        """
        relevant_store_info = self.get_store_attributes(Consts.STORE_ATTRIBUTES_TO_FILTER_BY)
        relevant_data = self.template_data[self.template_data.isin(relevant_store_info)]
        return relevant_data

    def get_relevant_scenes_by_params(self, template_names, template_groups):
        """
        This function returns the relevant scene_fks to calculate
        :param template_names: The relevant scene type from the template.
        :param template_groups: The relevant template group from the template.
        :return: List of scene fks.
        """
        filtered_scif = self.scif[['scene_fk', 'template_name', 'template_group']]
        if template_names:
            filtered_scif = filtered_scif[filtered_scif['template_name'].isin(template_names)]
        if template_groups:
            filtered_scif = filtered_scif[filtered_scif['template_group'].isin(template_groups)]

        return filtered_scif[Consts.SCENE_FK].unique().tolist()

    def get_general_filters(self, params):
        """
        This function returns the relevant KPI filters according to the template.
        :param params: The Atomic KPI row in the template
        :return: A dictionary with the relevant filters.
        """
        template_name = params[Consts.TEMPLATE_NAME].split(Consts.SEPARATOR)
        template_group = params[Consts.TEMPLATE_GROUP].split(Consts.SEPARATOR)
        relevant_scenes = self.get_relevant_scenes_by_params(template_name, template_group)
        params1 = params2 = params3 = []
        if params[Consts.PARAMS_VALUE_1]:
            params1 = map(unicode.strip, params[Consts.PARAMS_VALUE_1].split(Consts.SEPARATOR))
        if params[Consts.PARAMS_VALUE_2]:
            params2 = map(float, params[Consts.PARAMS_VALUE_2].split(Consts.SEPARATOR))
        if params[Consts.PARAMS_VALUE_3]:
            params3 = map(float, params[Consts.PARAMS_VALUE_3].split(Consts.SEPARATOR))

        result = {Consts.TARGET: params[Consts.TARGET],
                  Consts.SPLIT_SCORE: params[Consts.SPLIT_SCORE],
                  Consts.FILTERS: {
                      Consts.FILTER_PARAM_1: {params[Consts.PARAMS_TYPE_1]: params1},
                      Consts.FILTER_PARAM_2: {params[Consts.PARAMS_TYPE_2]: params2},
                      Consts.FILTER_PARAM_3: {params[Consts.PARAMS_TYPE_3]: params3},
                  }
                  }
        return result

    @staticmethod
    def validate_atomic_kpi(**filters):
        """
        TODO TODO TODO TODO TODO ?????
        :param filters: A dictionary with the relevant filters for the KPI.
        :return: True if everything is valid.
        """
        if filters.get(Consts.SPLIT_SCORE, 0) and not filters[Consts.SCENE_ID]:
            return False
        return True

    def get_identifier_result_kpi_by_name(self, kpi_type):
        """

        :param kpi_type:
        :return:
        """
        identifier_result = self.common.get_dictionary(kpi_fk=self.common.get_kpi_fk_by_kpi_type(kpi_type),
                                                       manufacturer_id=Consts.CBCIL_MANUFACTURER,
                                                       store_id=self.store_id)
        return identifier_result

    def get_kpi_weight(self, kpi, kpi_set):
        """

        :param kpi:
        :param kpi_set:
        :return:
        """
        row = self.kpi_weights[(self.kpi_weights[Consts.KPI_SET].str.encode('utf-8') == kpi_set.encode('utf-8')) &
                               (self.kpi_weights[Consts.KPI_NAME].str.encode('utf-8') == kpi.encode('utf-8'))]
        weight = row.get(Consts.WEIGHT)
        return weight.values[0] if not weight.empty else 0

    def get_kpi_details(self, kpi_set, kpi_fk, kpi, scores):
        """

        :param kpi_set:
        :param kpi_fk:
        :param kpi:
        :param scores:
        :return:
        """
        kpi_details = dict()
        kpi_details['kpi_fk'] = kpi_fk
        kpi_details['atomic_scores_and_weights'] = scores
        kpi_details['kpi_weight'] = self.get_kpi_weight(kpi, kpi_set)
        return kpi_details

    def calculate_eye_level(self, **general_filters):
        """

        :param general_filters:
        :return:
        """

        return 100

    def calculate_availability_from_bottom(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        shelf_number = int(general_filters.get(Consts.TARGET, 1))
        general_filters['filters']['All'].update({'shelf_number_from_bottom': range(shelf_number + 1)[1:]})
        score = self.calculate_availability(**general_filters)
        return score

    def calculate_kpi_result(self, kpi, atomic_results):
        """

        :param kpi:
        :param atomic_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return:
        """
        scores = []
        return scores

    def calculate_brand_block(self, **general_filters):
        """
        This function calculates the brand block KPI. It filters and excluded products according to the template and
        than checks if at least one scene has a block.
        :param general_filters:
        :return: 100 if at least one scene has a block, 0 otherwise.
        """
        allowed_products_dict = self.get_allowed_product_by_params(general_filters['filters'])
        block_result = self.block.calculate_block_together(allowed_products_filters=allowed_products_dict,
                                                           include_empty=False, result_by_scene=False,
                                                           minimum_block_ratio=Consts.MIN_BLOCK_RATIO,
                                                           min_facings_in_block=Consts.MIN_FACINGS_IN_BLOCK)
        return 100 if block_result else 0

    def get_allowed_product_by_params(self, filters):
        """
        This function include the relevant products for the block together KPI and exclude the ones that needs to be
        excluded by the template.
        :param filters: Atomic KPI filters.
        :return: A Dictionary with the relevant products. E.g: {'product_fk': [1,2,3,4,5]}.
        """
        allowed_product = dict([Consts.PRODUCT_FK, []])
        include_filter_key, include_filter_value = filters[Consts.FILTER_PARAM_1], filters[Consts.PARAMS_VALUE_1]
        exclude_filter_key, exclude_filter_value = filters[Consts.FILTER_PARAM_3], filters[Consts.PARAMS_VALUE_3]
        # TODO: FIX FILTERS
        filtered_scif = self.scif.loc[
            (self.scif[include_filter_key]).isin([include_filter_value]) & ~(self.scif[exclude_filter_key]).isin(
                [exclude_filter_value])]
        allowed_product[Consts.PRODUCT_FK] = filtered_scif[Consts.PRODUCT_FK].unique().tolist()
        return allowed_product

    def calculate_survey(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        if not general_filters[Consts.SCENE_ID]:
            return None
        params = general_filters[Consts.FILTERS]
        filters = params[Consts.PARAMS_VALUE_2].copy()
        survey_question = str(int(filters.get(Consts.QUESTION_ID)[0]))
        target_answers = general_filters[Consts.TARGET].split(Consts.SEPARATOR)
        survey_answer = self.survey.get_survey_answer((Consts.CODE, [survey_question]))
        if survey_answer:
            return 100 if survey_answer.strip() in target_answers else False
        else:
            return 0

    def calculate_availability(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        params = general_filters['filters']
        if params['All'][Consts.SCENE_ID]:
            filters = params[Consts.PARAMS_VALUE_1].copy()
            filters.update(params['All'])
            if self.availability.calculate_availability(**filters) >= 1:
                return 100
        return 0



