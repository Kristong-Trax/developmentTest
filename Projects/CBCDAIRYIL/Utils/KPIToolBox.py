from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils.ParseTemplates import parse_template
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.CBCDAIRYIL.Utils.Consts import Consts
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]

        self.survey = Survey(self.data_provider)
        self.block = Block(self.data_provider)
        self.general_toolbox = GENERALToolBox(self.data_provider)

        self.gap_data = self.get_gap_data()
        self.kpi_weights = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_WEIGHT, lower_headers_row_index=0)
        self.template_data = self.parse_template_data()
        self.kpis_gaps = list()
        self.passed_availability = []

    @staticmethod
    def get_gap_data():
        """
        This function parse the gap data template and returns the gap priorities.
        :return: A dict with the priorities according to kpi_names. E.g: {kpi_name1: 1, kpi_name2: 2 ...}
        """
        gap_sheet = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_GAP, lower_headers_row_index=0)
        gap_data = zip(gap_sheet[Consts.KPI_NAME], gap_sheet[Consts.ORDER])
        gap_data = {kpi_name: int(order) for kpi_name, order in gap_data}
        return gap_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        At first it fetches the relevant Sets (according to the stores attributes) and go over all of the relevant
        Atomic KPIs based on the project's template.
        Than, It aggregates the result per KPI using the weights and at last aggregates for the set level.
        """
        if self.template_data.empty:
            Log.warning(Consts.EMPTY_TEMPLATE_DATA_LOG.format(self.store_id))
            return
        kpi_set, kpis_list = self.get_relevant_kpis_for_calculation()
        kpi_set_fk = self.common.get_kpi_fk_by_kpi_type(Consts.TOTAL_SCORE)
        total_set_scores = list()
        for kpi_name in kpis_list:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
            atomics_df = self.get_atomics_to_calculate(kpi_name)
            atomic_results = self.calculate_atomic_results(kpi_fk, atomics_df)  # Atomic level
            kpi_results = self.calculate_kpis_and_save_to_db(atomic_results, kpi_fk, kpi_set_fk)  # KPI level
            kpi_weight = self.get_kpi_weight(kpi_name, kpi_set)
            total_set_scores.append((kpi_results, kpi_weight))
        self.calculate_kpis_and_save_to_db(total_set_scores, kpi_set_fk)  # Set level
        self.handle_gaps()

    def add_gap(self, atomic_kpi, score):
        """
        In case the score is not perfect the gap is added to the gap list.
        :param score: Atomic KPI score.
        :param atomic_kpi: A Series with data about the Atomic KPI.
        """
        current_gap_dict = dict()
        current_gap_dict[Consts.KPI_NAME] = atomic_kpi[Consts.KPI_NAME]
        current_gap_dict[Consts.KPI_ATOMIC_NAME] = atomic_kpi[Consts.KPI_ATOMIC_NAME]
        current_gap_dict[Consts.PRIORITY] = self.gap_data[atomic_kpi[Consts.KPI_NAME]]
        current_gap_dict[Consts.SCORE] = score
        self.kpis_gaps.append(current_gap_dict)

    @staticmethod
    def sort_by_priority(gap_dict):
        """ This is a util function for the kpi's gaps sorting by priorities"""
        return gap_dict[Consts.PRIORITY], gap_dict[Consts.SCORE]

    def handle_gaps(self):
        """
        This function takes the top 5 gaps (by priority) and saves it to the DB (pservice.custom_gaps table).
        :return:
        """
        self.kpis_gaps.sort(key=self.sort_by_priority)
        for gap in self.kpis_gaps[:5]:
            kpi_name, atomic_name, priority = gap[Consts.KPI_NAME], gap[Consts.KPI_ATOMIC_NAME], gap[Consts.PRIORITY]
            gap_query = Consts.GAPS_QUERY.format(self.session_fk, kpi_name, atomic_name, priority)
            self.common.execute_custom_query(gap_query)
            # Todo: Optional: change to one query with values?

    def calculate_kpis_and_save_to_db(self,  kpi_results, kpi_fk, parent_fk=None):
        """
        This KPI aggregates the score by weights and saves the results to the DB.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :param kpi_fk: The relevant KPI fk.
        :param parent_fk: The KPI SET FK that the KPI "belongs" too if exist.
        """
        kpi_score = self.calculate_kpi_result_by_weight(kpi_results)
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=Consts.CBCIL_MANUFACTURER,
                                       denominator_id=self.store_id,
                                       identifier_parent=parent_fk,
                                       result=kpi_score, score=kpi_score, should_enter=True)
        return kpi_score

    def calculate_kpi_result_by_weight(self, kpi_results):
        """
        This function aggregates the KPI results by scores and weights.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return: The aggregated KPI score.
        """
        weights_list = map(lambda res: res[1], kpi_results)
        if None in weights_list:  # No weights at all - dividing equally by length!
            kpi_score = sum(map(lambda res: res[0], kpi_results)) / len(kpi_results)
        elif sum(weights_list) < 1:  # Missing weights that needs to be divided among the kpis
            kpi_score = self.divide_missing_percentage(kpi_results, sum(weights_list))
        else:
            kpi_score = sum([score * weight for score, weight in kpi_results])
        return kpi_score

    @staticmethod
    def divide_missing_percentage(kpi_results, total_weights):
        """
        This function is been activated in case the total number of KPI weights doesn't equal to 100%.
        It divides the missing percentage among the other KPI and calculates the score.
        :param total_weights: The total number of weights that were calculated earlier.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return: KPI aggregated score.
        """
        missing_weight = 1 - total_weights
        weight_addition = missing_weight / len(kpi_results) if kpi_results else 0
        kpi_score = sum([score * (weight+weight_addition) for score, weight in kpi_results])
        return kpi_score

    def calculate_atomic_results(self, kpi_fk, atomics_df):
        """
        This method calculates the result for every atomic KPI (the lowest level) that are relevant for the kpi_fk.
        :param kpi_fk: The KPI FK that the atomic "belongs" too.
        :param atomics_df: The relevant Atomic KPIs from the project's template.
        :return: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        """
        total_scores = list()
        for i in atomics_df.index:
            current_atomic = atomics_df.loc[i]
            kpi_type = current_atomic.get(Consts.KPI_TYPE)
            general_filters = self.get_general_filters(current_atomic)
            atomic_weight = float(current_atomic.get(Consts.WEIGHT)) if current_atomic.get(Consts.WEIGHT) else None
            num_result = denominator_result = 0
            if general_filters is None:
                continue
            elif kpi_type in [Consts.AVAILABILITY]:
                atomic_score = self.calculate_availability(**general_filters)
            elif kpi_type == Consts.AVAILABILITY_FROM_BOTTOM:
                atomic_score = self.calculate_availability_from_bottom(**general_filters)
            elif kpi_type == Consts.MIN_2_AVAILABILITY:
                num_result, denominator_result, atomic_score = self.calculate_min_2_availability(**general_filters)
            elif kpi_type == Consts.SURVEY:
                atomic_score = self.calculate_survey(**general_filters)
            elif kpi_type == Consts.BRAND_BLOCK:
                atomic_score = self.calculate_brand_block(**general_filters)
            elif kpi_type == Consts.EYE_LEVEL:
                atomic_score = self.calculate_eye_level(**general_filters)
            else:
                Log.warning(Consts.UNSUPPORTED_KPI_LOG.format(kpi_type))
                continue
            if atomic_score is None:  # In cases that we need to ignore the KPI and divide it's weight
                continue
            elif atomic_score < 100:
                self.add_gap(current_atomic, atomic_score)
            total_scores.append((atomic_score, atomic_weight))

            atomic_fk_lvl_2 = self.common.get_kpi_fk_by_kpi_type(current_atomic[Consts.KPI_ATOMIC_NAME].strip())
            self.common.write_to_db_result(fk=atomic_fk_lvl_2, numerator_id=Consts.CBCIL_MANUFACTURER,
                                           numerator_result=num_result, denominator_id=self.store_id,
                                           denominator_result=denominator_result, identifier_parent=kpi_fk,
                                           result=atomic_score, score=atomic_score, should_enter=True)
        return total_scores

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
        This method filters the KPIs data to be the relevant atomic KPIs.
        :param kpi_name: The hebrew KPI name from the template.
        :return: A DataFrame that contains data about the relevant Atomic KPIs.
        """
        atomics = self.template_data[
            self.template_data[Consts.KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')]
        return atomics

    def get_store_attributes(self, attributes_names):
        """
        This function encodes and returns the relevant store attribute.
        :param attributes_names: List of requested store attributes to return.
        :return: A dictionary with the requested attributes, E.g: {attr_name: attr_val, ...}
        """
        # Filter store attributes
        store_info_dict = self.store_info.iloc[0].to_dict()
        filtered_store_info = {store_att: store_info_dict[store_att] for store_att in attributes_names}
        return filtered_store_info

    def parse_template_data(self):
        """
        This function responsible to filter the relevant template data..
        :return: A DataFrame with filtered Data by store attributes.
        """
        kpis_template = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_SHEET, lower_headers_row_index=1)
        relevant_store_info = self.get_store_attributes(Consts.STORE_ATTRIBUTES_TO_FILTER_BY)
        filtered_data = self.filter_template_by_store_att(kpis_template, relevant_store_info)
        return filtered_data

    @staticmethod
    def filter_template_by_store_att(kpis_template, store_attributes):
        """
        This function gets a dictionary with store type, additional attribute 1, 2 and 3 and filters the template by it.
        :param kpis_template: KPI sheet of the project's template.
        :param store_attributes: {store_type: X, additional_attribute_1: Y, ... }.
        :return: A filtered DataFrame.
        """
        for store_att, store_val in store_attributes.iteritems():
            kpis_template = kpis_template[kpis_template[store_att].str.encode('utf-8') == store_val.encode('utf-8')]
        return kpis_template

    def get_relevant_scenes_by_params(self, params):
        """
        This function returns the relevant scene_fks to calculate.
        :param params: The Atomic KPI row filters from the template.
        :return: List of scene fks.
        """
        template_names = params[Consts.TEMPLATE_NAME].split(Consts.SEPARATOR)
        template_groups = params[Consts.TEMPLATE_GROUP].split(Consts.SEPARATOR)
        filtered_scif = self.scif[[Consts.SCENE_ID, 'template_name', 'template_group']]
        if template_names and any(template_names):
            filtered_scif = filtered_scif[filtered_scif['template_name'].isin(template_names)]
        if template_groups and any(template_groups):
            filtered_scif = filtered_scif[filtered_scif['template_group'].isin(template_groups)]
        return filtered_scif[Consts.SCENE_ID].unique().tolist()

    def get_general_filters(self, params):
        """
        This function returns the relevant KPI filters according to the template.
        :param params: The Atomic KPI row in the template
        :return: A dictionary with the relevant filters.
        """
        general_filters = {Consts.TARGET: params[Consts.TARGET],
                           Consts.SPLIT_SCORE: params[Consts.SPLIT_SCORE],
                           Consts.KPI_FILTERS: dict()}
        relevant_scenes = self.get_relevant_scenes_by_params(params)
        if not relevant_scenes:
            return None
        else:
            general_filters[Consts.KPI_FILTERS][Consts.SCENE_FK] = relevant_scenes
        if params[Consts.PARAMS_VALUE_1]:
            params1 = map(unicode.strip, params[Consts.PARAMS_VALUE_1].split(Consts.SEPARATOR))
            param_type = params[Consts.PARAMS_TYPE_1]
            general_filters[Consts.KPI_FILTERS][param_type] = params1
        if params[Consts.PARAMS_VALUE_2]:
            params2 = map(float, params[Consts.PARAMS_VALUE_2].split(Consts.SEPARATOR))
            param_type = params[Consts.PARAMS_TYPE_2]
            general_filters[Consts.KPI_FILTERS][param_type] = params2
        if params[Consts.PARAMS_VALUE_3]:   # Params 3 are for Exclusion only!
            params3 = map(float, params[Consts.PARAMS_VALUE_3].split(Consts.SEPARATOR))
            param_type = params[Consts.PARAMS_TYPE_2]
            general_filters[Consts.EXCLUDE][param_type] = (params3, Consts.EXCLUDE_VALUE)

        return general_filters

    def get_kpi_weight(self, kpi, kpi_set):
        """
        This method returns the KPI weight according to the project's template.
        :param kpi: The KPI name.
        :param kpi_set: Set KPI name.
        :return:
        """
        row = self.kpi_weights[(self.kpi_weights[Consts.KPI_SET].str.encode('utf-8') == kpi_set.encode('utf-8')) &
                               (self.kpi_weights[Consts.KPI_NAME].str.encode('utf-8') == kpi.encode('utf-8'))]
        weight = row.get(Consts.WEIGHT)
        return weight.values[0] if not weight.empty else None

    def calculate_eye_level(self, **general_filters):
        """

        :param general_filters: A dictionary with the relevant KPI filters.
        :return:
        """
        filtered_scif = self.calculate_availability(return_df=True, **general_filters)
        total_number_of_facings = filtered_scif[Consts.FACINGS].sum()
        # Adding Golden Shelves filters
        general_filters[Consts.KPI_FILTERS].update({Consts.SHELF_NUM_FROM_BOTTOM: Consts.GOLDEN_SHELVES})
        golden_shelves_matches = self.get_filtered_matches(**general_filters)
        score = len(golden_shelves_matches) / float(total_number_of_facings) if total_number_of_facings else 0
        return 100 if score >= 0.75 else score

    def get_filtered_matches(self, **general_filters):
        """
        This function return match product in scene after it was filtered.
        :param general_filters: Filters to filter match product in scene by.
        :return: A filtered matches DataFrame.
        """
        kpi_filters = general_filters[Consts.KPI_FILTERS]
        filtered_matches = self.match_product_in_scene[
            self.general_toolbox.get_filter_condition(self.match_product_in_scene, **kpi_filters)]
        return filtered_matches

    def calculate_availability_from_bottom(self, **general_filters):
        """
        This function checks if *all* of the relevant products are in the lowest shelf.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return:
        """
        filtered_matches = self.get_filtered_matches(**general_filters)
        shelves_in_filtered_matches = filtered_matches[Consts.SHELF_NUM_FROM_BOTTOM].unique().tolist()
        # Check bottom shelf condition
        if not all(shelf == Consts.LOWEST_SHELF for shelf in shelves_in_filtered_matches):
            return 0
        products_filtered_matches = filtered_matches[Consts.EAN_CODE].unique().tolist()
        products_to_check = general_filters[Consts.KPI_FILTERS][Consts.PARAMS_TYPE_1]
        return 100 if len(products_to_check) == len(products_filtered_matches) else 0

    def calculate_brand_block(self, **general_filters):
        """
        This function calculates the brand block KPI. It filters and excluded products according to the template and
        than checks if at least one scene has a block.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: 100 if at least one scene has a block, 0 otherwise.
        """
        allowed_products_dict = self.get_allowed_product_by_params(**general_filters)
        block_result = self.block.calculate_block_together(allowed_products_filters=allowed_products_dict,
                                                           include_empty=False, result_by_scene=False,
                                                           minimum_block_ratio=Consts.MIN_BLOCK_RATIO,
                                                           min_facings_in_block=Consts.MIN_FACINGS_IN_BLOCK)
        return 100 if block_result else 0

    def get_allowed_product_by_params(self, **filters):
        """
        This function filters the relevant products for the block together KPI and exclude the ones that needs to be
        excluded by the template.
        :param filters: Atomic KPI filters.
        :return: A Dictionary with the relevant products. E.g: {'product_fk': [1,2,3,4,5]}.
        """
        allowed_product = dict()
        include_filters, exclude_filters = filters[Consts.KPI_FILTERS], filters[Consts.EXCLUDE]
        filtered_scif = self.calculate_availability(return_df=True, **dict(include_filters, **exclude_filters))
        allowed_product[Consts.PRODUCT_FK] = filtered_scif[Consts.PRODUCT_FK].unique().tolist()
        return allowed_product

    def calculate_survey(self, **general_filters):
        """
        This function calculates the result for Survey KPI.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: 100 if the answer is yes, else 0.
        """
        if Consts.QUESTION_ID not in general_filters[Consts.KPI_FILTERS].keys():
            Log.warning(Consts.MISSING_QUESTION_LOG)
            return 0
        survey_question = general_filters[Consts.KPI_FILTERS].get(Consts.QUESTION_ID)
        target_answer = general_filters[Consts.TARGET]
        survey_answer = self.survey.get_survey_answer(([survey_question], Consts.CODE))
        if survey_answer in Consts.SURVEY_ANSWERS_TO_IGNORE:
            return None
        elif survey_answer:
            return 100 if survey_answer.strip() == target_answer else 0
        return 0

    def calculate_availability(self, return_df=False, **general_filters):
        """
        This functions checks for availability by filters.
        During the calculation, if the KPI was passed, the results is being saved for future usage of
        "MIN 2 AVAILABILITY KPI".
        :param return_df: If True, the function returns the filtered scene item facts, else, returns the score.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: See @param return_df.
        """
        filtered_scif = self.scif[
            self.general_toolbox.get_filter_condition(self.scif, **general_filters[Consts.KPI_FILTERS])]
        if return_df:
            return filtered_scif
        if not filtered_scif.empty:
            tested_products = general_filters[Consts.KPI_FILTERS][Consts.EAN_CODE]
            self.passed_availability.append(tested_products)
            return 100
        return 0

    def calculate_min_2_availability(self, **general_filters):
        """
        This KPI checks for all of the Availability Atomics KPIs that passed, if the tested products have at least
        2 facings in case of IGNORE STACKING!
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: numerator result, denominator result and total_score
        """
        score = 0
        filtered_df = self.calculate_availability(return_df=True, **general_filters)
        filtered_df = filtered_df[filtered_df[Consts.FACINGS_IGN_STACK] > 1]
        passed_products_list = filtered_df[Consts.EAN_CODE].unique().tolist()
        for products in self.passed_availability:
            score += 1 if (all(x in passed_products_list for x in products)) else 0
        total_score = score / float(len(self.passed_availability)) if self.passed_availability else 0
        return score, float(len(self.passed_availability)), total_score
