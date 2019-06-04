from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils.ParseTemplates import parse_template
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.CBCDAIRYIL.Utils.Consts import Consts
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
import pandas as pd

__author__ = 'idanr'


class CBCDAIRYILToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.common = Common(self.data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_fk = self.data_provider.session_id
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
        self.passed_availability = list()

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
            kpi_weight = self.get_kpi_weight(kpi_name, kpi_set)
            atomics_df = self.get_atomics_to_calculate(kpi_name)
            atomic_results = self.calculate_atomic_results(kpi_fk, atomics_df)  # Atomic level
            kpi_results = self.calculate_kpis_and_save_to_db(atomic_results, kpi_fk, kpi_weight, kpi_set_fk)  # KPI lvl
            total_set_scores.append(kpi_results)
        self.calculate_kpis_and_save_to_db(total_set_scores, kpi_set_fk)  # Set level
        self.handle_gaps()

    def add_gap(self, atomic_kpi, score, atomic_weight):
        """
        In case the score is not perfect the gap is added to the gap list.
        :param atomic_weight: The Atomic KPI's weight.
        :param score: Atomic KPI score.
        :param atomic_kpi: A Series with data about the Atomic KPI.
        """
        parent_kpi_name = atomic_kpi[Consts.KPI_NAME]
        atomic_name = atomic_kpi[Consts.KPI_ATOMIC_NAME]
        atomic_fk = self.common.get_kpi_fk_by_kpi_type(atomic_name)
        current_gap_dict = {Consts.ATOMIC_FK: atomic_fk, Consts.PRIORITY: self.gap_data[parent_kpi_name],
                            Consts.SCORE: score, Consts.WEIGHT: atomic_weight}
        self.kpis_gaps.append(current_gap_dict)

    @staticmethod
    def sort_by_priority(gap_dict):
        """ This is a util function for the kpi's gaps sorting by priorities"""
        return gap_dict[Consts.PRIORITY], gap_dict[Consts.SCORE]

    def handle_gaps(self):
        """ This function takes the top 5 gaps (by priority) and saves it to the DB (pservice.custom_gaps table) """
        self.kpis_gaps.sort(key=self.sort_by_priority)
        gaps_total_score = 0
        gaps_per_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GAP_PER_ATOMIC_KPI)
        gaps_total_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GAPS_TOTAL_SCORE_KPI)
        for gap in self.kpis_gaps[:5]:
            current_gap_score = gap[Consts.WEIGHT] - (gap[Consts.SCORE] * gap[Consts.WEIGHT])
            gaps_total_score += current_gap_score
            self.insert_gap_results(gaps_per_kpi_fk, current_gap_score, gap[Consts.WEIGHT],
                                    numerator_id=gap[Consts.ATOMIC_FK], parent_fk=gaps_total_score_kpi_fk)
        total_weight = sum(map(lambda res: res[Consts.WEIGHT], self.kpis_gaps[:5]))
        self.insert_gap_results(gaps_total_score_kpi_fk, gaps_total_score, total_weight)

    def insert_gap_results(self, gap_kpi_fk, score, weight, numerator_id=Consts.CBC_MANU, parent_fk=None):
        """ This is a utility function that insert results to the DB for the GAP """
        should_enter = True if parent_fk else False
        score, weight = score*100, round(weight*100, 2)
        self.common.write_to_db_result(fk=gap_kpi_fk, numerator_id=numerator_id, numerator_result=score,
                                       denominator_id=self.store_id, denominator_result=weight, weight=weight,
                                       identifier_result=gap_kpi_fk, identifier_parent=parent_fk, result=score,
                                       score=score, should_enter=should_enter)

    def calculate_kpis_and_save_to_db(self,  kpi_results, kpi_fk, parent_kpi_weight=1.0, parent_fk=None):
        """
        This KPI aggregates the score by weights and saves the results to the DB.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :param kpi_fk: The relevant KPI fk.
        :param parent_kpi_weight: The parent's KPI total weight.
        :param parent_fk: The KPI SET FK that the KPI "belongs" too if exist.
        :return: The aggregated KPI score.
        """
        should_enter = True if parent_fk else False
        ignore_weight = not should_enter    # Weights should be ignored only in the set level!
        kpi_score = self.calculate_kpi_result_by_weight(kpi_results, parent_kpi_weight, ignore_weights=ignore_weight)
        total_weight = round(parent_kpi_weight*100, 2)
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=Consts.CBC_MANU, numerator_result=kpi_score,
                                       denominator_id=self.store_id, denominator_result=total_weight,
                                       identifier_result=kpi_fk, identifier_parent=parent_fk, should_enter=should_enter,
                                       weight=total_weight, result=kpi_score, score=kpi_score)
        return kpi_score

    def calculate_kpi_result_by_weight(self, kpi_results, parent_kpi_weight, ignore_weights=False):
        """
        This function aggregates the KPI results by scores and weights.
        :param ignore_weights: If True the function just sums the results.
        :param parent_kpi_weight: The parent's KPI total weight.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return: The aggregated KPI score.
        """
        if ignore_weights or len(kpi_results) == 0:
            return sum(kpi_results)
        weights_list = map(lambda res: res[1], kpi_results)
        if None in weights_list:  # Ignoring weights and dividing equally by length!
            kpi_score = sum(map(lambda res: res[0], kpi_results)) / float(len(kpi_results))
        elif round(sum(weights_list), 2) < parent_kpi_weight:  # Missing weights needs to be divided among the kpis
            kpi_score = self.divide_missing_percentage(kpi_results, parent_kpi_weight, sum(weights_list))
        else:
            kpi_score = sum([score * weight for score, weight in kpi_results])
        return kpi_score

    @staticmethod
    def divide_missing_percentage(kpi_results, parent_weight, total_weights):
        """
        This function is been activated in case the total number of KPI weights doesn't equal to 100%.
        It divides the missing percentage among the other KPI and calculates the score.
        :param parent_weight: Parent KPI's weight.
        :param total_weights: The total number of weights that were calculated earlier.
        :param kpi_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return: KPI aggregated score.
        """
        missing_weight = parent_weight - total_weights
        weight_addition = missing_weight / float(len(kpi_results)) if kpi_results else 0
        kpi_score = sum([score * (weight + weight_addition) for score, weight in kpi_results])
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
            kpi_type, atomic_weight, general_filters = self.get_relevant_data_per_atomic(current_atomic)
            if general_filters is None:
                continue
            num_result, den_result, atomic_score = self.calculate_atomic_kpi_by_type(kpi_type, **general_filters)
            # Handling Atomic KPIs results
            if atomic_score is None:  # In cases that we need to ignore the KPI and divide it's weight
                continue
            elif atomic_score < 100:
                self.add_gap(current_atomic, atomic_score, atomic_weight)
            total_scores.append((atomic_score, atomic_weight))
            atomic_fk_lvl_2 = self.common.get_kpi_fk_by_kpi_type(current_atomic[Consts.KPI_ATOMIC_NAME].strip())
            self.common.write_to_db_result(fk=atomic_fk_lvl_2, numerator_id=Consts.CBC_MANU,
                                           numerator_result=num_result, denominator_id=self.store_id,
                                           weight=round(atomic_weight*100, 2), denominator_result=den_result,
                                           should_enter=True, identifier_parent=kpi_fk,
                                           result=atomic_score, score=atomic_score * atomic_weight)

        return total_scores

    def get_relevant_data_per_atomic(self, atomic_series):
        """
        This function return the relevant data per Atomic KPI.
        :param atomic_series: The Atomic row from the Template.
        :return: A tuple with data: (atomic_type, atomic_weight, general_filters)
        """
        kpi_type = atomic_series.get(Consts.KPI_TYPE)
        atomic_weight = float(atomic_series.get(Consts.WEIGHT)) if atomic_series.get(Consts.WEIGHT) else None
        general_filters = self.get_general_filters(atomic_series)
        return kpi_type, atomic_weight, general_filters

    def calculate_atomic_kpi_by_type(self, atomic_type, **general_filters):
        """
        This function calculates the result according to the relevant Atomic Type.
        :param atomic_type: KPI Family from the template.
        :param general_filters: Relevant attributes and values to calculate by.
        :return: A tuple with results: (numerator_result, denominator_result, total_score).
        """
        num_result = denominator_result = 0
        if atomic_type in [Consts.AVAILABILITY]:
            atomic_score = self.calculate_availability(**general_filters)
        elif atomic_type == Consts.AVAILABILITY_FROM_BOTTOM:
            atomic_score = self.calculate_availability_from_bottom(**general_filters)
        elif atomic_type == Consts.MIN_2_AVAILABILITY:
            num_result, denominator_result, atomic_score = self.calculate_min_2_availability(**general_filters)
        elif atomic_type == Consts.SURVEY:
            atomic_score = self.calculate_survey(**general_filters)
        elif atomic_type == Consts.BRAND_BLOCK:
            atomic_score = self.calculate_brand_block(**general_filters)
        elif atomic_type == Consts.EYE_LEVEL:
            num_result, denominator_result, atomic_score = self.calculate_eye_level(**general_filters)
        else:
            Log.warning(Consts.UNSUPPORTED_KPI_LOG.format(atomic_type))
            atomic_score = None
        return num_result, denominator_result, atomic_score

    def get_relevant_kpis_for_calculation(self):
        """
        This function retrieve the relevant KPIs to calculate from the template
        :return: A tuple: (set_name, [kpi1, kpi2, kpi3...]) to calculate.
        """
        kpi_set = self.template_data[Consts.KPI_SET].values[0]
        kpis = self.template_data[self.template_data[Consts.KPI_SET].str.encode('utf-8') ==
                                  kpi_set.encode('utf-8')][Consts.KPI_NAME].unique().tolist()
        # Planogram KPI should be calculated last because of the MINIMUM 2 FACINGS KPI.
        if Consts.PLANOGRAM_KPI in kpis and kpis.index(Consts.PLANOGRAM_KPI) != len(kpis) - 1:
            kpis.append(kpis.pop(kpis.index(Consts.PLANOGRAM_KPI)))
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
        Filter params 1 & 2 are included and param 3 is for exclusion.
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
            general_filters[Consts.KPI_FILTERS][Consts.SCENE_ID] = relevant_scenes
        for type_col, value_col in Consts.KPI_FILTER_VALUE_LIST:
            if params[value_col]:
                should_included = Consts.INCLUDE_VAL if value_col != Consts.PARAMS_VALUE_3 else Consts.EXCLUDE_VAL
                param_type, param_value = params[type_col], params[value_col]
                filter_param = self.handle_param_values(param_type, param_value)
                general_filters[Consts.KPI_FILTERS][param_type] = (filter_param, should_included)

        return general_filters

    @staticmethod
    def handle_param_values(param_type, param_value):
        """
        :param param_type: The param type to filter by. E.g: product_ean code or brand_name
        :param param_value: The value to filter by.
        :return: list of param values.
        """
        values_list = param_value.split(Consts.SEPARATOR)
        params = map(lambda val: float(val) if unicode.isdigit(val) and param_type != Consts.EAN_CODE else val.strip(),
                     values_list)
        return params

    def get_kpi_weight(self, kpi, kpi_set):
        """
        This method returns the KPI weight according to the project's template.
        :param kpi: The KPI name.
        :param kpi_set: Set KPI name.
        :return: The kpi weight (Float).
        """
        row = self.kpi_weights[(self.kpi_weights[Consts.KPI_SET].str.encode('utf-8') == kpi_set.encode('utf-8')) &
                               (self.kpi_weights[Consts.KPI_NAME].str.encode('utf-8') == kpi.encode('utf-8'))]
        weight = row.get(Consts.WEIGHT)
        return float(weight.values[0]) if not weight.empty else None

    def merge_and_filter_scif_and_matches_for_eye_level(self, **kpi_filters):
        """
        This function merges between scene_item_facts and match_product_in_scene DataFrames and filters the merged DF
        according to the @param kpi_filters.
        :param kpi_filters: Dictionary with attributes and values to filter the DataFrame by.
        :return: The merged and filtered DataFrame.
        """
        scif_matches_diff = self.match_product_in_scene[['scene_fk', 'product_fk'] +
                                                        list(self.match_product_in_scene.keys().difference(
                                                            self.scif.keys()))]
        merged_df = pd.merge(self.scif[self.scif.facings != 0], scif_matches_diff, how='outer',
                             left_on=['scene_id', 'item_id'], right_on=[Consts.SCENE_FK, Consts.PRODUCT_FK])
        merged_df = merged_df[self.general_toolbox.get_filter_condition(merged_df, **kpi_filters)]
        return merged_df

    def calculate_eye_level(self, **general_filters):
        """
        This function calculates the Eye level KPI. It filters and products according to the template and
        returns a Tuple: (eye_level_facings / total_facings, score).
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: E.g: (10, 20, 50) or (8, 10, 100) --> score >= 75 turns to 100.
        """
        merged_df = self.merge_and_filter_scif_and_matches_for_eye_level(**general_filters[Consts.KPI_FILTERS])
        total_number_of_facings = len(merged_df)
        merged_df = self.filter_df_by_shelves(merged_df, Consts.EYE_LEVEL_PER_SHELF)
        eye_level_facings = len(merged_df)
        total_score = eye_level_facings / float(total_number_of_facings) if total_number_of_facings else 0
        total_score = 100 if total_score >= 0.75 else total_score
        return eye_level_facings, total_number_of_facings, total_score

    @staticmethod
    def filter_df_by_shelves(df, eye_level_definition):
        """
        This function filters the df according to the eye-level definition
        :param df: data frame to filter
        :param eye_level_definition: definition for eye level shelves
        :return: filtered data frame
        """
        number_of_shelves = df.shelf_number_from_bottom.max()
        top, bottom = 0, 0
        for json_def in eye_level_definition:
            if json_def[Consts.MIN] <= number_of_shelves <= json_def[Consts.MAX]:
                top = json_def[Consts.TOP]
                bottom = json_def[Consts.BOTTOM]
        return df[(df.shelf_number_from_bottom <= number_of_shelves - top) & (df.shelf_number_from_bottom > bottom)]

    def calculate_availability_from_bottom(self, **general_filters):
        """
        This function checks if *all* of the relevant products are in the lowest shelf.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return:
        """
        allowed_products_dict = self.get_allowed_product_by_params(**general_filters)
        filtered_matches = self.match_product_in_scene[
            self.match_product_in_scene[Consts.PRODUCT_FK].isin(allowed_products_dict[Consts.PRODUCT_FK])]
        relevant_shelves_to_check = set(filtered_matches[Consts.SHELF_NUM_FROM_BOTTOM].unique().tolist())
        # Check bottom shelf condition
        return 0 if len(relevant_shelves_to_check) != 1 or Consts.LOWEST_SHELF not in relevant_shelves_to_check else 100

    def calculate_brand_block(self, **general_filters):
        """
        This function calculates the brand block KPI. It filters and excluded products according to the template and
        than checks if at least one scene has a block.
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: 100 if at least one scene has a block, 0 otherwise.
        """
        products_dict = self.get_allowed_product_by_params(**general_filters)
        block_result = self.block.network_x_block_together(
                                population=products_dict,
                                additional={'minimum_block_ratio': Consts.MIN_BLOCK_RATIO,
                                            'minimum_facing_for_block': Consts.MIN_FACINGS_IN_BLOCK,
                                            'allowed_products_filters': {'product_type': ['Empty']},
                                            'calculate_all_scenes': False,
                                            'include_stacking': True,
                                            'check_vertical_horizontal': False})

        result = 100 if not block_result.empty and not block_result[block_result.is_block].empty else 0
        return result

    def get_allowed_product_by_params(self, **filters):
        """
        This function filters the relevant products for the block together KPI and exclude the ones that needs to be
        excluded by the template.
        :param filters: Atomic KPI filters.
        :return: A Dictionary with the relevant products. E.g: {'product_fk': [1,2,3,4,5]}.
        """
        allowed_product = dict()
        filtered_scif = self.calculate_availability(return_df=True, **filters)
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
        survey_question_id = general_filters[Consts.KPI_FILTERS].get(Consts.QUESTION_ID)
        # General filters returns output for filter_df basically so we need to adjust it here.
        if isinstance(survey_question_id, tuple):
            survey_question_id = survey_question_id[0]  # Get rid of the tuple
        if isinstance(survey_question_id, list):
            survey_question_id = int(survey_question_id[0])     # Get rid of the list
        target_answer = general_filters[Consts.TARGET]
        survey_answer = self.survey.get_survey_answer((Consts.QUESTION_FK, survey_question_id))
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
            tested_products = general_filters[Consts.KPI_FILTERS][Consts.EAN_CODE][0]
            self.passed_availability.append(tested_products)
            return 100
        return 0

    @staticmethod
    def get_number_of_facings_per_product_dict(df, ignore_stack=False):
        """
        This function gets a DataFrame and returns a dictionary with number of facings per products.
        :param df: Pandas.DataFrame with 'product_ean_code' and 'facings' / 'facings_ign_stack' fields.
        :param ignore_stack: If True will use 'facings_ign_stack' field, else 'facings' field.
        :return: E.g: {ean_code1: 10, ean_code2: 5, ean_code3: 1...}
        """
        stacking_field = Consts.FACINGS_IGN_STACK if ignore_stack else Consts.FACINGS
        df = df[[Consts.EAN_CODE, stacking_field]].dropna()
        df = df[df[stacking_field] > 0]
        facings_dict = dict(zip(df[Consts.EAN_CODE], df[stacking_field]))
        return facings_dict

    def calculate_min_2_availability(self, **general_filters):
        """
        This KPI checks for all of the Availability Atomics KPIs that passed, if the tested products have at least
        2 facings in case of IGNORE STACKING!
        :param general_filters: A dictionary with the relevant KPI filters.
        :return: numerator result, denominator result and total_score
        """
        score = 0
        filtered_df = self.calculate_availability(return_df=True, **general_filters)
        facings_counter = self.get_number_of_facings_per_product_dict(filtered_df, ignore_stack=True)
        for products in self.passed_availability:
            score += 1 if sum(
                [facings_counter[product] for product in products if product in facings_counter]) > 1 else 0
        total_score = (score / float(len(self.passed_availability)))*100 if self.passed_availability else 0
        return score, len(self.passed_availability), total_score
