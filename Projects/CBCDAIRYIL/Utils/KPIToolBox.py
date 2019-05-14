
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
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox

__author__ = 'idanr'


class CBCDAIRYILToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.store_additional_attr_1 = self.get_store_attribute(Consts.ADDITIONAL_ATTRIBUTE_1)
        self.store_type = self.get_store_attribute(Consts.STORE_TYPE)

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

    def get_store_attribute(self, attribute_name):
        """
        This function encodes and returns the relevant store attribute.
        :param attribute_name: Attribute name from the data_provider[Data.STORE_INFO].
        :return: The relevant attribute with encode (if necessary).
        """
        store_att = self.store_info.at[0, attribute_name]
        if not store_att:
            Log.warning("The store attribute {} doesn't exist for store fk = {}".format(attribute_name, self.store_id))
            return None
        if isinstance(store_att, (unicode, str)):
            store_att = store_att.encode('utf-8')
        return store_att

    def filter_template_data(self):
        """
        This function responsible to handle the encoding issue we have in the template because of the Hebrew language.
        :return: Same template data with encoding.
        """
        relevant_data = self.kpis_data[(self.kpis_data[Consts.STORE_TYPE].str.encode('utf-8').isin(self.store_type)) &
                                       (self.kpis_data[Consts.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').isin(
                                           self.store_additional_attr_1))]
        return relevant_data

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

        :param kpi_name:
        :return:
        """
        atomics = self.template_data[self.template_data[Consts.KPI_NAME].str.encode('utf-8') ==
                                     kpi_name.encode('utf-8')]
        return atomics

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

    def calculate_atomic_results(self, kpi_fk, atomics_df):
        """
        This method calculates the result for every atomic KPI (the lowest level) that are relevant for the kpi_fk.
        :param atomics_df: The relevant Atomic KPIs from the project's template.
        :return: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        """
        # identifier_result_kpi = self.get_identifier_result_kpi_by_name(kpi)
        scores = list()
        for i in atomics_df.index:
            current_atomic = atomics_df.iloc[i]
            kpi_type = current_atomic.get(Consts.KPI_TYPE)  # TODO: CHECK FOR SINGLE ATOMIC
            general_filters = self.get_general_filters(current_atomic)
            atomic_weight = float(float(current_atomic.get(Consts.WEIGHT))) if current_atomic.get(
                Consts.WEIGHT) else None
            if not self.validate_atomic_kpi(**general_filters):
                continue
            if kpi_type == Consts.BLOCK_BY_TOP_SHELF:
                shelf_number = int(general_filters.get(Consts.TARGET, 1))
                general_filters['filters']['All'].update({'shelf_number': range(shelf_number + 1)[1:]})
                atomic_score = self.calculate_block_by_shelf(**general_filters)
            elif kpi_type == Consts.SOS:
                atomic_score = self.calculate_sos(**general_filters)
            elif kpi_type == Consts.SOS_COOLER:
                atomic_score = self.calculate_sos_cooler(**general_filters)
            elif kpi_type in [Consts.AVAILABILITY or Consts.AVAILABILITY_FROM_MID_AND_UP]:
                atomic_score = self.calculate_availability(**general_filters)
            elif kpi_type == Consts.AVAILABILITY_BY_SEQUENCE:
                atomic_score = self.calculate_availability_by_sequence(**general_filters)
            elif kpi_type == Consts.AVAILABILITY_BY_TOP_SHELF:
                atomic_score = self.calculate_availability_by_top_shelf(**general_filters)
            elif kpi_type == Consts.AVAILABILITY_FROM_BOTTOM:
                shelf_number = int(general_filters.get(Consts.TARGET, 1))
                general_filters['filters']['All'].update({'shelf_number_from_bottom': range(shelf_number + 1)[1:]})
                atomic_score = self.calculate_availability(**general_filters)
            elif kpi_type == Consts.MIN_2_AVAILABILITY:
                atomic_score = self.calculate_min_2_availability(**general_filters)
            elif kpi_type == Consts.SURVEY:
                atomic_score = self.calculate_survey(**general_filters)
            else:
                Log.warning("KPI of type '{}' is not supported".format(kpi_type))
                continue

            if atomic_score == 0:
                self.add_gap(current_atomic)
            scores.append((atomic_score, atomic_weight))

            atomic_fk_lvl_2 = self.common.get_kpi_fk_by_kpi_type(current_atomic[Consts.KPI_ATOMIC_NAME])
            self.common.write_to_db_result(fk=atomic_fk_lvl_2, numerator_id=Consts.CBCIL_MANUFACTURER,
                                           denominator_id=self.store_id,
                                           identifier_parent=identifier_result_kpi,
                                           result=atomic_score, score=atomic_score, should_enter=True)
        return scores

    def calculate_kpi_result(self, kpi, atomic_results):
        """

        :param kpi:
        :param atomic_results: A list of results and weights tuples: [(score1, weight1), (score2, weight2) ... ].
        :return:
        """
        scores = []
        return scores

    def main_calculation(self):
        """
        This function calculates the KPI results.
        At first it fetches the relevant Sets (according to the stores attributes) and go over all of the relevant
        Atomic KPIs based on the project's template.
        Than, It aggregates the result per KPI using the weights and at last aggregates for the set level.
        """
        if self.template_data.empty:
            Log.warning("No relevant data in the template for store fk = {}! Exiting...".format(self.store_id))
            return
        kpi_set, kpis_list = self.get_relevant_kpis_for_calculation()
        total_set_scores = list()
        for kpi_name in kpis_list:
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
            atomics_df = self.get_atomics_to_calculate(kpi_name)
            atomic_results = self.calculate_atomic_results(kpi_fk, atomics_df)  # Atomic level
            kpi_results = self.calculate_kpi_result(kpi_name, atomic_results)   # KPI level
            total_set_scores.append(self.get_kpi_details(kpi_set, kpi_fk, kpi_name, kpi_results))

        score = 0
        return score

    def calculate_availability_by_top_shelf(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        params = general_filters['filters']
        if params['All'][Consts.SCENE_ID]:
            shelf_number = int(general_filters.get(Consts.TARGET, 1))
            shelf_numbers = range(shelf_number + 1)[1:]
            if shelf_numbers:
                session_results = []
                for scene in params['All'][Consts.SCENE_ID]:
                    scene_result = 0
                    scif_filters = {'scene_fk': scene}
                    scif_filters.update(params[Consts.PARAMS_VALUE_1])
                    scif_filters.update(params[Consts.PARAMS_VALUE_2])
                    scif = self.scif.copy()
                    scene_skus = scif[self.general_toolbox.get_filter_condition(scif, **scif_filters)][
                        'product_fk'].unique().tolist()
                    if scene_skus:
                        matches_filters = {'scene_fk': scene}
                        matches_filters.update({'product_fk': scene_skus})
                        matches_filters.update({'shelf_number': shelf_numbers})
                        matches = self.match_product_in_scene.copy()
                        result = matches[self.general_toolbox.get_filter_condition(matches, **matches_filters)]
                        if not result.empty:
                            shelf_facings_result = result.groupby('shelf_number')['scene_fk'].count().values.tolist()
                            if len(shelf_facings_result) == len(shelf_numbers):
                                target_facings_per_shelf = params[Consts.PARAMS_VALUE_3]['facings'][0]
                                scene_result = 100 if all([facing >= target_facings_per_shelf
                                                           for facing in shelf_facings_result]) else 0
                    session_results.append(scene_result)
                return 100 if any(session_results) else 0
        return 0

    def calculate_availability_by_sequence(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        params = general_filters['filters']
        if params['All'][Consts.SCENE_ID]:
            filters = params[Consts.PARAMS_VALUE_1].copy()
            filters.update(params[Consts.PARAMS_VALUE_2])
            filters.update(params['All'])
            matches = self.match_product_in_scene.merge(self.scif, on='product_fk')
            result = matches[self.general_toolbox.get_filter_condition(matches, **filters)]
            if not result.empty:
                result = result['shelf_number'].unique().tolist()
                result.sort()
            if len(result) >= 4:
                for i in range(len(result) - 3):
                    if result[i] == result[i + 1] - 1 == result[i + 2] - 2 == result[i + 3] - 3:
                        return 100
        return 0

    def calculate_block_by_shelf(self, **general_filters):
        """

        :param general_filters:
        :return:
        """
        params = general_filters['filters']
        if params['All'][Consts.SCENE_ID]:
            filters = params[Consts.PARAMS_VALUE_1].copy()
            filters.update(params[Consts.PARAMS_VALUE_2])
            filters.update(params[Consts.PARAMS_VALUE_3])
            filters.update(params['All'])
            for scene in params['All'][Consts.SCENE_ID]:
                filters.update({Consts.SCENE_ID: scene})
                try:
                    filters.pop('')
                except:     # todo todo todo: FIX! understand the exception!
                    pass
                block = self.block.calculate_block_together(include_empty=False,
                                                            minimum_block_ratio=Consts.MIN_BLOCK_RATIO,
                                                            allowed_products_filters={
                                                                Consts.PRODUCT_TYPE: Consts.OTHER},
                                                            vertical=True, **filters)
                if not isinstance(block, dict):
                    return 0
                elif float(len(block['shelves'])) >= float(general_filters[Consts.TARGET]):
                    return 100
        return 0

    def calculate_sos_cooler(self, general_filters):
        """

        :param general_filters:
        :return:
        """
        cbc_coolers, competitor_coolers, cbc_scenes = self.get_coolers(Consts.CBC_COOLERS, Consts.COMPETITOR_COOLERS)
        params = general_filters[Consts.FILTERS]
        if general_filters[Consts.SCENE_ID]:
            numerator_filters = params[Consts.FILTER_PARAM_1].copy()
            numerator_filters.update(params[Consts.FILTER_PARAM_2])

            set_scores = []
            for scene in cbc_scenes:
                current_scene_filters = {Consts.SCENE_FK: scene}
                ratio = self.sos.calculate_linear_share_of_shelf(numerator_filters, **current_scene_filters)
                set_scores.append(ratio)
            set_scores.sort()

            if competitor_coolers > 0 and cbc_coolers > 0:
                return sum(set_scores) / len(set_scores)*100
            elif cbc_coolers > 1:
                if all(score < 0.8 for score in set_scores):
                    set_scores.sort(reverse=True)
                return (min(set_scores[0] / 0.8, 1) + sum(set_scores[1:])) / len(set_scores) * 100
            elif cbc_coolers == 1:
                return set_scores[0]/0.8*100 if set_scores[0] < 0.8 else 100
        return 0

    def get_coolers(self, cbc_cooler, competitor_cooler):
        """

        :param cbc_cooler:
        :param competitor_cooler:
        :return:
        """
        cbc = self.scif[self.scif['template_name'].str.encode('utf-8') == cbc_cooler][Consts.SCENE_FK].unique().tolist()
        competitor = self.scif[self.scif['template_name'].str.encode('utf-8').isin(competitor_cooler)][
            Consts.SCENE_FK].unique()
        return len(cbc), len(competitor), cbc

    def calculate_sos(self, **general_filters):
        """
        TODO TODO TODO TODO TODO ????????????????????
        :param general_filters:
        :return:
        """
        if general_filters[Consts.SCENE_ID]:
            params = general_filters[Consts.FILTERS]
            numerator_filters = params[Consts.PARAMS_VALUE_1].copy()
            numerator_filters.update(params[Consts.PARAMS_VALUE_2])
            numerator_filters.update(params[Consts.PARAMS_VALUE_3])
            ratio = self.sos.calculate_linear_share_of_shelf(numerator_filters,
                                                             include_empty=True,
                                                             **params['All'])

            if ratio >= float(general_filters[Consts.TARGET]):
                return 100
        return 0

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
