import os
import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.Utils import ToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CCZA_SAND.Utils.Fetcher import CCZAQueries
from Projects.CCZA_SAND.Utils.ParseTemplates import parse_template
from Projects.CCZA_SAND.Utils.Const import Const
from Projects.CCZA_SAND.Utils.Converters import Converters
from KPIUtils.GeneralToolBox import GENERALToolBox
from KPIUtils.DB.Common import Common
from KPIUtils.Calculations.Survey import Survey
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Utils.Parsers.ParseInputKPI import filter_df
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts, MatchesConsts

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class CCZAToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        # self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        query_store_type = CCZAQueries.get_attr3(self.session_uid)
        store_type = pd.read_sql_query(query_store_type, self.rds_conn.db)
        self.store_type = store_type[Const.ATTR3].iloc[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.tools = GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.tool_box_for_flow = ToolBox
        # create data-frames from template
        self.kpi_sheets = {}
        for name in Const.sheet_names_and_rows:
            self.kpi_sheets[name] = parse_template(TEMPLATE_PATH, sheet_name=name,
                                                   lower_headers_row_index=Const.sheet_names_and_rows[name])
        self.common = Common(self.data_provider, Const.RED_SCORE)
        self.survey_handler = Survey(self.data_provider, self.output, self.kpi_sheets[Const.SURVEY_QUESTIONS])
        self.kpi_static_data = self.common.kpi_static_data
        self.kpi_results_queries = []
        self.common_v2 = CommonV2(self.data_provider)
        self.own_manuf_fk = self.get_own_manufacturer_fk()
        self.scif_match_react = self.scif[self.scif[ScifConsts.RLV_SOS_SC] == 1]

    def get_own_manufacturer_fk(self):
        own_manufacturer_fk = self.data_provider.own_manufacturer.param_value.values[0]
        # own_manufacturer_fk = self.all_products[self.all_products['manufacturer_name'] ==
        #                                         'MARS GCC']['manufacturer_fk'].values[0]
        return int(float(own_manufacturer_fk))

    def sos_main_calculation(self):
        store_sos_ident_par, store_facings = self.calculate_own_manufacturer_out_of_store()
        category_df = self.calculate_sos_category_out_of_store(store_sos_ident_par, store_facings)
        manufacturer_cat_df = self.calculate_sos_manufacturer_out_of_category(category_df)
        self.calculate_sos_brand_out_of_manufacturer(manufacturer_cat_df)

    def calculate_own_manufacturer_out_of_store(self):
        manuf_out_of_store_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_OWN_MANUF_OUT_OF_STORE)
        store_sos_ident_par = self.common_v2.get_dictionary(kpi_fk=manuf_out_of_store_fk)
        num_result = self.scif_match_react[self.scif_match_react[ScifConsts.MANUFACTURER_FK] == self.own_manuf_fk] \
                                                                [ScifConsts.FACINGS_IGN_STACK].sum()
        denom_result = float(self.scif_match_react[ScifConsts.FACINGS_IGN_STACK].sum())
        sos_result = num_result / denom_result if denom_result else 0
        # num_filters, denom_filters = self.construct_sos_filters(('manufacturer', self.own_manuf_fk), ('', ''))
        # num_filters = {ScifConsts.MANUFACTURER_FK: self.own_manuf_fk}
        # denom_filters = {}
        # num_result, denom_result, sos_result = self.calculate_sos_custom(num_filters, denom_filters,
        #                                                                  Const.IGNORE_STACKING)
        self.common_v2.write_to_db_result(fk=manuf_out_of_store_fk, numerator_id=self.own_manuf_fk,
                                          denominator_id=self.store_id, numerator_result=num_result,
                                          denominator_result=denom_result, score=sos_result * 100, result=sos_result,
                                          identifier_result=store_sos_ident_par, should_enter=True)
        return store_sos_ident_par, denom_result

    def calculate_sos_brand_out_of_manufacturer(self, manuf_df):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_BRAND_OUT_CAT)
        brand_man_cat_df = self.scif_match_react.groupby([ScifConsts.CATEGORY_FK, ScifConsts.MANUFACTURER_FK,
                                                          ScifConsts.BRAND_FK],
                                                         as_index=False).agg({ScifConsts.FACINGS_IGN_STACK: np.sum})
        brand_man_cat_df = brand_man_cat_df.merge(manuf_df, on=[ScifConsts.CATEGORY_FK, ScifConsts.MANUFACTURER_FK],
                                                  how='left')
        brand_man_cat_df['sos'] = brand_man_cat_df[ScifConsts.FACINGS_IGN_STACK] / brand_man_cat_df['man_cat_facings']
        for i, row in brand_man_cat_df.iterrows():
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.BRAND_FK],
                                              denominator_id=row[ScifConsts.MANUFACTURER_FK],
                                              numerator_result=row[ScifConsts.FACINGS_IGN_STACK],
                                              denominator_result=row['man_cat_facings'], result=row['sos'],
                                              context_id=row[ScifConsts.CATEGORY_FK],
                                              score=row['sos'] * 100, identifier_parent=row['manuf_id_parent'],
                                              should_enter=True)

    def calculate_sos_manufacturer_out_of_category(self, cat_df):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_MANUF_OUT_OF_CAT)
        manuf_in_cat_df = self.scif_match_react.groupby([ScifConsts.CATEGORY_FK, ScifConsts.MANUFACTURER_FK],
                                                        as_index=False).agg({ScifConsts.FACINGS_IGN_STACK: np.sum})
        manuf_in_cat_df = manuf_in_cat_df.merge(cat_df, on=ScifConsts.CATEGORY_FK, how='left')
        manuf_in_cat_df['sos'] = manuf_in_cat_df[ScifConsts.FACINGS_IGN_STACK] / manuf_in_cat_df['category_facings']
        manuf_in_cat_df['id_result'] = manuf_in_cat_df.apply(self.build_identifier_parent_man_in_cat,
                                                             axis=1, args=(kpi_fk,))
        for i, row in manuf_in_cat_df.iterrows():
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.MANUFACTURER_FK],
                                              denominator_id=row[ScifConsts.CATEGORY_FK],
                                              numerator_result=row[ScifConsts.FACINGS_IGN_STACK],
                                              denominator_result=row['category_facings'], result=row['sos'],
                                              score=row['sos'] * 100, identifier_parent=row['cat_id_parent'],
                                              identifier_result=row['id_result'],
                                              should_enter=True)
        manuf_in_cat_df.rename(columns={ScifConsts.FACINGS_IGN_STACK: 'man_cat_facings',
                                        'id_result': 'manuf_id_parent'}, inplace=True)
        manuf_in_cat_df = manuf_in_cat_df[[ScifConsts.MANUFACTURER_FK, ScifConsts.CATEGORY_FK,
                                           'man_cat_facings', 'manuf_id_parent']]
        return manuf_in_cat_df

    @staticmethod
    def build_identifier_parent_man_in_cat(row, kpi_fk):
        id_result_dict = {Const.KPI_FK: kpi_fk, ScifConsts.CATEGORY_FK: row[ScifConsts.CATEGORY_FK],
                          ScifConsts.MANUFACTURER_FK: row[ScifConsts.MANUFACTURER_FK]}
        return id_result_dict

    def calculate_sos_category_out_of_store(self, store_sos_ident_par, store_facings):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(Const.SOS_CAT_OUT_OF_STORE)
        cat_df = self.scif_match_react.groupby([ScifConsts.CATEGORY_FK],
                                               as_index=False).agg({ScifConsts.FACINGS_IGN_STACK: np.sum})
        cat_df['sos'] = cat_df[ScifConsts.FACINGS_IGN_STACK] / store_facings
        cat_df['id_result'] = cat_df[ScifConsts.CATEGORY_FK].apply(lambda x: {ScifConsts.CATEGORY_FK: x,
                                                                              Const.KPI_FK: kpi_fk})
        for i, row in cat_df.iterrows():
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.CATEGORY_FK],
                                              denominator_id=self.store_id,
                                              numerator_result=row[ScifConsts.FACINGS_IGN_STACK],
                                              denominator_result=store_facings, result=row['sos'],
                                              score=row['sos'] * 100, identifier_parent=store_sos_ident_par,
                                              identifier_result=row['id_result'], should_enter=True)
        cat_df.rename(columns={ScifConsts.FACINGS_IGN_STACK: 'category_facings', 'id_result': 'cat_id_parent'},
                      inplace=True)
        cat_df = cat_df[[ScifConsts.CATEGORY_FK, 'category_facings', 'cat_id_parent']]
        return cat_df

    # def calculate_sos_custom(self, num_filters_input, denom_filters_input, ignore_stacking):
    #     num_filters_input.update(denom_filters_input)
    #     num_filters = {'population': {'include': [num_filters_input]}}
    #     denom_filters = {'population': {'include': [denom_filters_input]}}
    #     num_result = self.calculate_facings_space(num_filters, ignore_stacking)
    #     denom_result = self.calculate_facings_space(denom_filters, ignore_stacking)
    #     sos_result = num_result / denom_result if denom_result else 0
    #     return num_result, denom_result, sos_result
    #
    # def calculate_facings_space(self, filters, ignore_stack_flag):
    #     filtered_scif = filter_df(filters, self.scif)
    #     length_field = ScifConsts.FACINGS_IGN_STACK if ignore_stack_flag else ScifConsts.FACINGS
    #     space_length = filtered_scif[length_field].sum()
    #     return float(space_length)

    # @staticmethod
    # def construct_sos_filters((num_entity, num_value), (denom_entity, denom_value)):
    #     num_filter_key = '{}_fk'.format(num_entity) if num_entity else None
    #     denom_filter_key = '{}_fk'.format(denom_entity) if denom_entity else None
    #
    #     num_filters = {num_filter_key: num_value} if num_filter_key is not None else {}
    #     denom_filters = {denom_filter_key: denom_value} if denom_filter_key is not None else {}
    #
    #     num_filters.update(denom_filters)
    #     return num_filters, denom_filters

    def main_calculation_red_score(self):
        set_score = 0
        try:
            set_name = self.kpi_sheets[Const.KPIS].iloc[len(self.kpi_sheets[Const.KPIS]) - 1][
                Const.KPI_NAME]
            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(set_name)
            set_identifier_res = self.common_v2.get_dictionary(kpi_fk=kpi_fk)
            if self.store_type in self.kpi_sheets[Const.KPIS].keys().tolist():
                for i in xrange(len(self.kpi_sheets[Const.KPIS]) - 1):
                    params = self.kpi_sheets[Const.KPIS].iloc[i]
                    percent = self.get_percent(params[self.store_type])
                    if percent == 0:
                        continue
                    kpi_score = self.main_calculation_lvl_2(identifier_parent=set_identifier_res, params=params)
                    set_score += kpi_score * percent
            else:
                Log.warning('The store-type "{}" is not recognized in the template'.format(self.store_type))
                return
            kpi_names = {Const.column_name1: set_name}
            set_fk = self.get_kpi_fk_by_kpi_path(self.common.LEVEL1, kpi_names)
            if set_fk:
                try:
                    self.common.write_to_db_result(score=set_score, level=self.common.LEVEL1, fk=set_fk)
                except Exception as exception:
                    Log.error('Exception in the set {} writing to DB: {}'.format(set_name, exception.message))
            self.common_v2.write_to_db_result(fk=kpi_fk, numerator_id=self.own_manuf_fk,
                                              denominator_id=self.store_id, score=set_score,
                                              result=set_score, identifier_result=set_identifier_res,
                                              should_enter=True)
        except Exception as exception:
            Log.error('Exception in the kpi-set calculating: {}'.format(exception.message))
            pass

    def main_calculation_lvl_2(self, identifier_parent,  *args, **kwargs):
        """
            :param kwargs: dict - kpi line from the template.
            the function gets the kpi (level 2) row, and calculates its children.
            :return: float - score of the kpi.
        """
        kpi_params = kwargs['params']
        kpi_name = kpi_params[Const.KPI_NAME]
        kpi_score = 0.0
        set_name = kpi_params[Const.KPI_GROUP]
        kpi_type = kpi_params[Const.KPI_TYPE]
        target = kpi_params[Const.TARGET]
        kpi_fk_lvl_2 = self.common_v2.get_kpi_fk_by_kpi_type(kpi_name)
        lvl_2_identifier_par = self.common_v2.get_dictionary(kpi_fk=kpi_fk_lvl_2)
        if kpi_name != Const.FLOW:
            for i in xrange(len(self.kpi_sheets[target])):
                if kpi_params[Const.WEIGHT_SHEET].strip():
                    atomic_params = self.kpi_sheets[kpi_params[Const.WEIGHT_SHEET]].iloc[i]
                    atomic_params[Const.targets_line] = self.kpi_sheets[target].iloc[i]
                else:
                    atomic_params = self.kpi_sheets[target].iloc[i]
                percent = self.get_percent(atomic_params[self.store_type])
                if percent == 0.0 or atomic_params[Const.KPI_NAME] != kpi_name:
                    continue
                atomic_params[Const.type] = kpi_type
                atomic_score = self.calculate_atomic(atomic_params, set_name, lvl_2_identifier_par)
                if atomic_score is None:
                    atomic_score = 0.0
                    Log.error('The calculated score is not good.')
                kpi_score += atomic_score * percent
        else:
            atomic_row = self.kpi_sheets[target].iloc[0]
            atomic_params = atomic_row.to_dict()
            atomic_params.update({Const.ATOMIC_NAME: kpi_name, Const.KPI_NAME: kpi_name, Const.type: kpi_name})
            # kpi_score = self.calculate_atomic({Const.ATOMIC_NAME: kpi_name,
            #                                    Const.KPI_NAME: kpi_name,
            #                                    Const.type: kpi_name}, set_name, lvl_2_identifier_par)
            kpi_score = self.calculate_atomic(atomic_params, set_name, lvl_2_identifier_par)
        kpi_names = {Const.column_name1: set_name, Const.column_name2: kpi_name}
        kpi_fk = self.get_kpi_fk_by_kpi_path(self.common.LEVEL2, kpi_names)
        if kpi_fk:
            try:
                self.common.write_to_db_result(score=kpi_score, level=self.common.LEVEL2, fk=kpi_fk)
            except Exception as e:
                Log.error('Exception in the kpi {} writing to DB: {}'.format(kpi_name, e.message))
        self.common_v2.write_to_db_result(fk=kpi_fk_lvl_2, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                          score=kpi_score, result=kpi_score, identifier_parent=identifier_parent,
                                          identifier_result=lvl_2_identifier_par, should_enter=True)
        return kpi_score

    def calculate_atomic(self, atomic_params, set_name, lvl_2_identifier_parent):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :param set_name: str - name of the set, for DB.
            the function gets the atomic (level 3) row, and calculates it by the options (availability/SOS/flow/survey).
            :return: float - score of the atomic kpi.
        """
        atomic_name = atomic_params[Const.ATOMIC_NAME]
        kpi_name = atomic_params[Const.KPI_NAME]
        atomic_type = atomic_params[Const.type]
        if atomic_type == Const.AVAILABILITY:
            atomic_score = self.calculate_availability(atomic_params)
        elif atomic_type == Const.SOS_FACINGS:
            atomic_score = self.calculate_sos(atomic_params)
        elif atomic_type == Const.SURVEY_QUESTION:
            if Const.KPI_TYPE in atomic_params.keys() and atomic_params[Const.KPI_TYPE] != Const.SURVEY:
                atomic_score = self.calculate_survey_with_types(atomic_params)
            else:
                atomic_score = self.calculate_survey_with_codes(atomic_params)
        elif atomic_type == Const.FLOW:
            atomic_score = self.calculate_flow(atomic_params)
        else:
            atomic_score = 0.0
            Log.error('The type "{}" is unknown'.format(atomic_type))
        kpi_names = {Const.column_name1: set_name, Const.column_name2: kpi_name, Const.column_name3: atomic_name}
        atomic_fk = self.get_kpi_fk_by_kpi_path(self.common.LEVEL3, kpi_names)
        if atomic_fk:
            try:
                self.common.write_to_db_result(score=atomic_score, level=self.common.LEVEL3, fk=atomic_fk)
            except Exception as e:
                Log.error('Exception in the atomic-kpi {} writing to DB: {}'.format(atomic_name, e.message))
        kpi_fk_lvl_3 = self.common_v2.get_kpi_fk_by_kpi_type(atomic_name) if atomic_name != Const.FLOW \
            else self.common_v2.get_kpi_fk_by_kpi_type(Const.FLOW_LVL_3)
        self.common_v2.write_to_db_result(fk=kpi_fk_lvl_3, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                          result=atomic_score, score=atomic_score,
                                          identifier_parent=lvl_2_identifier_parent, should_enter=True)
        return atomic_score

    @kpi_runtime()
    def calculate_availability(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: 100 if this product is available, 0 otherwise.
        """
        availability = self.calculate_all_availability(atomic_params[Const.ENTITY_TYPE],
                                                       atomic_params[Const.ENTITY_VAL],
                                                       atomic_params[Const.ENTITY_TYPE2],
                                                       atomic_params[Const.ENTITY_VAL2],
                                                       atomic_params[Const.IN_NOT_IN],
                                                       Converters.convert_type(atomic_params[Const.TYPE_FILTER]),
                                                       atomic_params[Const.VALUE_FILTER])
        return 100.0 * (availability > 0)

    def calculate_all_availability(self, type1, value1, type2, value2, in_or_not, filter_type, filter_value):
        """
            :param atomic_params: dict - atomic kpi line from the template.
            checks the kind of the survey and sends it to the match function
            :return: float - score.
        """
        if not type1 or not value1:
            Log.warning('There is no type and value in the atomic availability')
            return 0.0
        type1 = Converters.convert_type(type1)
        value1 = value1.split(', ')
        value1 = map(lambda x: x.strip(), value1)
        type2 = Converters.convert_type(type2)
        value2 = value2
        filters = {type1: value1}
        if type2 and value2:
            value2 = value2.split(', ')
            value2 = map(lambda x: x.strip(), value2)
            filters[type2] = value2
        if in_or_not:
            filters = self.update_filters(filters, in_or_not, filter_type, filter_value)
        return self.tools.calculate_availability(**filters)

    @kpi_runtime()
    def calculate_survey_with_types(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template.
            checks the kind of the survey and sends it to the match function
            :return: float - score.
        """
        atomic_score = 0.0
        accepted_answer = atomic_params[Const.ACCEPTED_ANSWER_RESULT]
        atomic_type = atomic_params[Const.KPI_TYPE]
        if not accepted_answer:
            if atomic_type == Const.AVAILABILITY:
                accepted_answer = 1
            else:
                Log.warning('There is no accepted answer to {} in the template'.format(
                    atomic_params[Const.ATOMIC_NAME]))
                return atomic_score
        if atomic_type == Const.AVAILABILITY:
            availability = self.calculate_all_availability(atomic_params[Const.ENTITY_TYPE],
                                                           atomic_params[Const.ENTITY_VAL],
                                                           atomic_params[Const.ENTITY_TYPE2],
                                                           atomic_params[Const.ENTITY_VAL2],
                                                           atomic_params[Const.IN_NOT_IN],
                                                           atomic_params[Const.TYPE_FILTER],
                                                           atomic_params[Const.VALUE_FILTER])
            atomic_score = 100.0 * (availability >= float(accepted_answer))
        elif atomic_type == Const.SCENE_COUNT:
            count = self.calculate_scene_count(atomic_params)
            atomic_score = 100.0 * (count >= float(accepted_answer))
        elif atomic_type == Const.PLANOGRAM:
            atomic_score = self.calculate_planogram_new(atomic_params)
        else:
            Log.warning('The type "{}" is not recognized'.format(atomic_type))
        return atomic_score

    @kpi_runtime()
    def calculate_survey_with_codes(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template.
            checks survey question.
            :return: float - score.
        """
        atomic_score = 0.0
        try:
            code = str(int(float(atomic_params[Const.SURVEY_Q_CODE])))
        except ValueError:
            Log.warning('The atomic "{}" has no survey code'.format(atomic_params[Const.ATOMIC_NAME]))
            return atomic_score
        survey_text = atomic_params[Const.ATOMIC_NAME]
        if Const.targets_line in atomic_params.keys():
            try:
                accepted_answer = float(atomic_params[Const.targets_line][self.store_type])
            except ValueError:
                Log.warning('The atomic "{}" has no target'.format(atomic_params[Const.ATOMIC_NAME]))
                return atomic_score
            survey_data = self.tools.survey_response[self.tools.survey_response['code'].isin([code])]
            if survey_data.empty:
                Log.warning('Survey with {} = {} does not exist'.format('code', code))
                survey_data = self.tools.survey_response[
                    self.tools.survey_response['question_text'].isin([survey_text])]
                if survey_data.empty:
                    return atomic_score
            survey_answer = survey_data['number_value'].iloc[0]
            check_survey = accepted_answer >= survey_answer
        else:
            survey_code_couple = ('code', code)
            accepted_answer = atomic_params[Const.ACCEPTED_ANSWER_RESULT]
            check_survey = self.tools.check_survey_answer(survey_code_couple, accepted_answer)
            if check_survey is None:
                check_survey = self.tools.check_survey_answer(survey_text, accepted_answer)
                if check_survey is None:
                    return atomic_score
        atomic_score = 100.0 * check_survey
        return atomic_score

    def calculate_planogram(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: 100 if there is planogram, 0 otherwise.
        """
        type_name = Converters.convert_type(atomic_params[Const.ENTITY_TYPE])
        values = atomic_params[Const.ENTITY_VAL].split(', ')
        wanted_answer = float(atomic_params[Const.ACCEPTED_ANSWER_RESULT])
        filtered_scenes = self.scif[self.scif[type_name].isin(values)]['scene_id'].unique()
        count = 0
        for scene_id in filtered_scenes:
            query = CCZAQueries.getPlanogramByTemplateName(scene_id)
            planogram = pd.read_sql_query(query, self.rds_conn.db)
            if 1 in planogram['match_compliance_status'].unique().tolist():
                count += 1
            if count >= wanted_answer:
                return 100.0
        return 0.0

    def calculate_planogram_new(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: 100 if there is scene which has at least one correctly positioned product, 0 otherwise.
        """
        type_name = Converters.convert_type(atomic_params[Const.ENTITY_TYPE])
        values = map(lambda x: x.strip(), atomic_params[Const.ENTITY_VAL].split(', '))
        wanted_answer = float(atomic_params[Const.ACCEPTED_ANSWER_RESULT])
        filtered_scenes = self.scif[self.scif[type_name].isin(values)][ScifConsts.SCENE_FK].unique()
        scenes_passing = 0
        for scene in filtered_scenes:
            incor_tags = self.match_product_in_scene[(self.match_product_in_scene[ScifConsts.SCENE_FK]
                                                      == scene) &
                                                     (~(self.match_product_in_scene[MatchesConsts.COMPLIANCE_STATUS_FK]
                                                      == 3))]
            if len(incor_tags) == 0:
                scenes_passing += 1
        score = 100 if scenes_passing >= wanted_answer else 0
        # p_matches = self.match_product_in_scene[self.match_product_in_scene[ScifConsts.SCENE_FK].isin(filtered_scenes)]
        # planogram_matches_passing = p_matches[~(p_matches[MatchesConsts.COMPLIANCE_STATUS_FK] == 3)]
        # scenes_passing = len(planogram_matches_passing[ScifConsts.SCENE_FK].unique())
        # score = 100 if scenes_passing >= wanted_answer else 0
        return score

    def calculate_scene_count(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: int - amount of scenes.
        """
        filters = {Converters.convert_type(
            atomic_params[Const.ENTITY_TYPE]): map(lambda x: x.strip(), atomic_params[Const.ENTITY_VAL].split(', '))}
        scene_count = self.tools.calculate_number_of_scenes(**filters)
        return scene_count

    @kpi_runtime()
    def calculate_sos(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: the percent of SOS (if it's binary - 100 if more than target, otherwise 0).
        """
        numerator_type = atomic_params[Const.ENTITY_TYPE_NUMERATOR]
        numerator_value = atomic_params[Const.NUMERATOR]
        denominator_type = atomic_params[Const.ENTITY_TYPE_DENOMINATOR]
        denominator_value = atomic_params[Const.DENOMINATOR]
        in_or_not = atomic_params[Const.IN_NOT_IN]
        filter_type = Converters.convert_type(atomic_params[Const.TYPE_FILTER])
        filter_value = atomic_params[Const.VALUE_FILTER]
        denominator_filters = self.get_default_filters(denominator_type, denominator_value)
        numerator_filters = self.get_default_filters(numerator_type, numerator_value)
        if in_or_not:
            numerator_filters = self.update_filters(numerator_filters, in_or_not, filter_type, filter_value)
            denominator_filters = self.update_filters(denominator_filters, in_or_not, filter_type, filter_value)
        atomic_score = self.tools.calculate_share_of_shelf(
            sos_filters=numerator_filters, **denominator_filters) * 100
        if atomic_params[Const.SCORE] == Const.BINARY:
            try:
                return 100 * (atomic_score >= float(atomic_params[Const.targets_line][
                                                        self.store_type]))
            except ValueError:
                Log.warning('The target for {} is bad in store {}'.format(
                    atomic_params[Const.ATOMIC_NAME], self.store_type))
                return 0.0
        elif atomic_params[Const.SCORE] != Const.NUMERIC:
            Log.error('The score is not numeric and not binary.')
        return atomic_score

    def update_filters(self, filters, in_or_not, filter_type, filter_value):
        """
            :param filters: the source filters as dict.
            :param in_or_not: boolean if it should include or not include
            :param filter_type: str
            :param filter_value: str
            adds to exist filter if to include/exclude one more condition.
            :return: dict - the updated filter.
        """
        filter_type = Converters.convert_type(filter_type)
        if "Not" in in_or_not:
            list_of_negative = list(self.scif[~(self.scif[filter_type] == filter_value)][filter_type].unique())
            filters[filter_type] = list_of_negative
        elif "In" in in_or_not:
            filters[filter_type] = filter_value
        else:
            Log.warning('The value in "In/Not In" in the template should be "Not in", "In" or empty')
        return filters

    @kpi_runtime()
    def calculate_flow(self, atomic_params):
        """
            checking if the shelf is sorted like the brands list.
            :return: 100 if it's fine, 0 otherwise.
        """

        # progression_list = ['COCA-COLA', 'COCA COLA PLUS COFFEE', 'COKE ZERO', 'COKE LIGHT',
        #                     'COCA COLA NO SUGAR NO CAFFEINE', 'TAB', 'SPRITE', 'SPRITE ZERO', 'FANTA ORANGE',
        #                     'Fanta Mango', 'FANTA Pinapple', 'FANTA Grape', 'STONEY']
        population_entity_type = Converters.convert_type(atomic_params[Const.ENTITY_TYPE])
        progression_list = map(lambda x: x.strip(), atomic_params[Const.ENTITY_VAL].split(','))
        location_entity_type = Converters.convert_type(atomic_params[Const.TYPE_FILTER])
        location_values = map(lambda x: x.strip(), atomic_params[Const.VALUE_FILTER].split(','))

        filtered_scif = self.scif[
            (~self.scif[location_entity_type].isin(location_values)) &
            (self.scif['tagged'] >= 1) &
            (self.scif[population_entity_type].isin(progression_list))]
        join_on = ['scene_fk', 'product_fk']
        match_product_join_scif = pd.merge(filtered_scif, self.match_product_in_scene, on=join_on, how='left',
                                           suffixes=('_x', '_matches'))
        progression_field = 'brand_name'
        group_column = 'scene_fk'

        progression_cross_shelves_true = self.tool_box_for_flow.progression(
            df=match_product_join_scif, progression_list=progression_list, progression_field=progression_field,
            at_least_one=False, left_to_right=True, cross_bays=True, cross_shelves=True,
            include_stacking=False, group_by=group_column)
        progression_cross_shelves_false = self.tool_box_for_flow.progression(
            df=match_product_join_scif, progression_list=progression_list, progression_field=progression_field,
            at_least_one=False, left_to_right=True, cross_bays=True, cross_shelves=False,
            include_stacking=False, group_by=group_column)

        return 100.0 * (progression_cross_shelves_true or
                        progression_cross_shelves_false)

    def get_kpi_fk_by_kpi_path(self, kpi_level, kpi_names):
        """
        :param kpi_level: int - kpi level.
        :param kpi_names: dict - all the path from the set.
        :return: int - the fk of this kpi in the DB.
        """
        for name in kpi_names:
            assert isinstance(kpi_names[name], (unicode, basestring)), "name is not a string: %r" % kpi_names[name]
        try:
            if kpi_level == self.common.LEVEL1:
                return self.kpi_static_data[self.kpi_static_data[Const.column_name1] == kpi_names[Const.column_name1]][
                    Const.column_key1].values[0]
            elif kpi_level == self.common.LEVEL2:
                return self.kpi_static_data[
                    (self.kpi_static_data[Const.column_name1] == kpi_names[Const.column_name1]) &
                    (self.kpi_static_data[Const.column_name2] == kpi_names[Const.column_name2])][
                    Const.column_key2].values[0]
            elif kpi_level == self.common.LEVEL3:
                return self.kpi_static_data[
                    (self.kpi_static_data[Const.column_name1].str.encode('utf8') == kpi_names[Const.column_name1].encode('utf8')) &
                    (self.kpi_static_data[Const.column_name2].str.encode('utf8') == kpi_names[Const.column_name2].encode('utf8')) &
                    (self.kpi_static_data[Const.column_name3].str.encode('utf8') == kpi_names[Const.column_name3].encode('utf8'))][
                    Const.column_key3].values[0]
            else:
                raise ValueError, 'invalid level'
        except IndexError:
            Log.info('Kpi path: {}, is not equal to any kpi name in static table'.format(kpi_names))
            return None

    @staticmethod
    def get_default_filters(type_name, value_name):
        """
            :param type_name: string that contains list of types
            :param value_name: string that contains list of values in the same length
            :return: filter as dict.
        """
        if ',' in type_name:
            types = type_name.split(', ')
            types = map(lambda x: x.strip(), types)
            values = value_name.split(', ')
            values = map(lambda x: x.strip(), values)
            filters = {}
            if len(types) != len(values):
                Log.warning('there are {} types and {} values, should be the same amount'.format(
                    len(types), len(values)))
            else:
                for i in xrange(len(types)):
                    filters[Converters.convert_type(types[i])] = values[i]
        else:
            filters = {Converters.convert_type(type_name): map(lambda x: x.strip(), value_name.split(', '))}
        # list_of_negative = list(self.scif[self.scif['rlv_sos_sc'] != 0]['rlv_sos_sc'].unique())
        # filters['rlv_sos_sc'] = list_of_negative # perhaps we don't need it - if we need, to enter it to the initializer
        return filters

    @staticmethod
    def get_percent(num):
        """
        :param num: a cell from the template, can be float in percent, int or string for 0
        :return: the number divided by 100
        """
        try:
            answer = float(num)
            ans = answer if answer < 1 else answer / 100.0
            if ans < 0 or ans > 100:
                Log.error('The weight is {} in the template, not possible.'.format(ans))
                return 0.0
            return ans
        except ValueError:
            return 0.0