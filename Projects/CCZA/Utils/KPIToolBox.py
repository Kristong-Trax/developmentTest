import os
import pandas as pd

from Trax.Algo.Calculations.Core.Utils import ToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.CCZA.Utils.Fetcher import CCZAQueries
from Projects.CCZA.Utils.ParseTemplates import parse_template
from Projects.CCZA.Utils.Const import Const
from Projects.CCZA.Utils.Converters import Converters
from KPIUtils.GeneralToolBox import GENERALToolBox
from KPIUtils.DB.Common import Common
from KPIUtils.Calculations.Survey import Survey

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

    def main_calculation(self, *args, **kwargs):
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
        if target.strip():
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
                atomic_score = self.calculate_atomic(atomic_params, set_name)
                if atomic_score is None:
                    atomic_score = 0.0
                    Log.error('The calculated score is not good.')
                kpi_score += atomic_score * percent
        elif kpi_name == Const.FLOW:
            kpi_score = self.calculate_atomic({Const.ATOMIC_NAME: kpi_name,
                                               Const.KPI_NAME: kpi_name,
                                               Const.type: kpi_name}, set_name)
        kpi_names = {Const.column_name1: set_name, Const.column_name2: kpi_name}
        kpi_fk = self.get_kpi_fk_by_kpi_path(self.common.LEVEL2, kpi_names)
        if kpi_fk:
            try:
                self.common.write_to_db_result(score=kpi_score, level=self.common.LEVEL2, fk=kpi_fk)
            except Exception as e:
                Log.error('Exception in the kpi {} writing to DB: {}'.format(kpi_name, e.message))
        return kpi_score

    def calculate_atomic(self, atomic_params, set_name):
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
            atomic_score = self.calculate_flow()
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
        return atomic_score

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
            atomic_score = self.calculate_planogram(atomic_params)
        else:
            Log.warning('The type "{}" is not recognized'.format(atomic_type))
        return atomic_score

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

    def calculate_scene_count(self, atomic_params):
        """
            :param atomic_params: dict - atomic kpi line from the template
            :return: int - amount of scenes.
        """
        filters = {Converters.convert_type(
            atomic_params[Const.ENTITY_TYPE]): atomic_params[Const.ENTITY_VAL].split(', ')}
        scene_count = self.tools.calculate_number_of_scenes(**filters)
        return scene_count

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
            list_of_negative = list(self.scif[self.scif[filter_type] != filter_value][filter_type].unique())
            filters[filter_type] = list_of_negative
        elif "In" in in_or_not:
            filters[filter_type] = filter_value
        else:
            Log.warning('The value in "In/Not In" in the template should be "Not in", "In" or empty')
        return filters

    def calculate_flow(self):
        """
            checking if the shelf is sorted like the brands list.
            :return: 100 if it's fine, 0 otherwise.
        """
        progression_list = ['COCA-COLA', 'COCA-COLA Life', 'COKE ZERO', 'COKE LIGHT', 'TAB', 'SPRITE',
                            'SPRITE ZERO', 'FANTA ORANGE', 'FANTA ZERO', 'FANTA Grape', 'FANTA Pinapple']

        filtered_scif = self.scif[
            (~self.scif['location_type'].isin(["Pricing Scene Types", "Not For Flow"])) &
            (self.scif['tagged'] >= 1) &
            (self.scif['brand_name'].isin(progression_list))]
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
            values = value_name.split(', ')
            filters = {}
            if len(types) != len(values):
                Log.warning('there are {} types and {} values, should be the same amount'.format(
                    len(types), len(values)))
            else:
                for i in xrange(len(types)):
                    filters[Converters.convert_type(types[i])] = values[i]
        else:
            filters = {Converters.convert_type(type_name): value_name.split(', ')}
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

