import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

# imports from PS Utils
import pandas as pd
from KPIUtils.DB.Common import Common
from KPIUtils.Calculations.Survey import Survey
from KPIUtils.Calculations.SOS import SOS
from KPIUtils.Calculations.Availability import Availability
from KPIUtils.Utils.Convertors.FilterHandler import FilterGenerator

# projct imports
from Projects.CCBOTTLERSUS.REDSCORE.GeneralToolBox import REDGENERALToolBox
from Projects.CCBOTTLERSUS.REDSCORE.Queries import Queries
from Projects.CCBOTTLERSUS.Utils.ParseTemplates import parse_template
from Projects.CCBOTTLERSUS.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS.REDSCORE.Converters import Converters
from Projects.CCBOTTLERSUS.REDSCORE.Checks import Checks


class REDToolBox:
    def __init__(self, data_provider, output, id):

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
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = REDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        # self.kpi_results_queries = []
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]

        # load helpers
        if id == 0:
            self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                              'COKE_UNITED_RS_KPI_Template_v5.1.xlsx')
            self.RED_SCORE = Const.RED_SCORE
            self.RED_SCORE_INTEG = Const.RED_SCORE_INTEG
        else:
            self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                              'MANUAL RED SURVEY_COKE_UNITED_RS_KPI_Template_v3.5.xlsx')
            self.RED_SCORE = Const.MANUAL_RED_SCORE
            self.RED_SCORE_INTEG = Const.MANUAL_RED_SCORE_INTEG

        self.common_db = Common(self.data_provider, self.RED_SCORE)
        self.common_db_integ = Common(self.data_provider, self.RED_SCORE_INTEG)
        self.common_db.delete_results_data_by_kpi_set()
        self.common_db_integ.delete_results_data_by_kpi_set()


        self.kpi_static_data = self.common_db.get_kpi_static_data()
        # self.kpi_convertor = KpiConverter(self.kpi_static_data)
        self.filter_gen = FilterGenerator()
        self.checks = Checks(self.data_provider)
        self.sos = SOS(data_provider=data_provider, output=output)
        self.availability = Availability(data_provider=data_provider, output=output)
        # load data set
        self.kpi_template = self._get_data_template(Const.KPIS_SHEET_NAME, 1)
        self.survey_template = self._get_data_template(Const.SURVEY_SHEET_NAME, 3)
        if id == 0:
            self.sku_availability_template = self._get_data_template(Const.SKU_AVAILABILITY_SHEET_NAME, 3)
            self.scene_type_sheet = self._get_data_template(Const.SCENE_AVAILABILITY_SHEET_NAME, 3)
            self.exculsion_sheet = self._get_data_template(Const.EXCLUSION_SHEET, 3)
            self.SOS_sheet = self._get_data_template(Const.SOS_NAME, 3)
            self.SOS_WITH_DP_sheet = self._get_data_template(Const.SOS_WITH_DP_NAME, 3)
        self.max_scores_dict = self._calculate_max_scores_dict()
        self.survey_handler = Survey(self.data_provider, self.output, self.survey_template)
        self.red_score_condition = []  # store all passed conditions
        self.score = 0

    @staticmethod
    def merge_two_dicts(x, y):
        z = x.copy()  # start with x's keys and values
        z.update(y)  # modifies z with y's keys and values & returns None
        return z

    def _calculate_max_scores_dict(self):
        store_type_list = self.kpi_template[self.kpi_template[Const.KPI_GROUP] == self.RED_SCORE][
            Const.STORE_TYPE].unique().tolist()

        weight_dict = {}

        for store_type in store_type_list:
            weight_df = self.kpi_template[(self.kpi_template[Const.KPI_GROUP] == self.RED_SCORE)
                                          & (self.kpi_template[Const.STORE_TYPE] == store_type)][Const.WEIGHT]
            weight_dict[store_type] = pd.to_numeric(weight_df).sum()
        return weight_dict

    def _get_store_attribute_15(self):
        query = Queries.get_store_attribute_15(self.store_id)
        result = pd.read_sql_query(query, self.rds_conn.db)
        return result[Converters.convert_column_to_store_filter(Const.STORE_ATT15)].iloc[0]

    def _get_product_with_att3(self, att3_name):
        att3_list = tuple([a.encode('ascii', 'ignore') for a in att3_name.split(',')])

        if len(att3_list) > 1:
            att3 = att3_list

        elif len(att3_list) == 1:
            att3 = att3_name.replace(',', '')
        else:
            return None

        query = Queries.get_list_of_products_with_att3(att3, tuple(self.scene_info.scene_fk.tolist()))
        result = pd.read_sql_query(query, self.rds_conn.db)
        return result['pk'].tolist()

    def _get_data_template(self, sheet_name, lower_headers_row_index=1):
        return parse_template(template_path=self.TEMPLATE_PATH,
                              sheet_name=sheet_name,
                              lower_headers_row_index=lower_headers_row_index)

    @staticmethod
    def _get_scene_type_scif_filter(filtered_template):
        return {'template_name': filtered_template[Const.SCENE_TYPE]}

    def _check_store_attribute(self, store_attribute, kpi_details):
        # TODO bring from scif
        if store_attribute == Const.STORE_ATT15:
            return True \
                if kpi_details[store_attribute] == self._get_store_attribute_15() else False

        else:
            return True \
                if kpi_details[store_attribute] == \
                   self.store_info[Converters.convert_column_to_store_filter(store_attribute)].iloc[0] \
                else False

    def _check_kpi_condition(self, kpi_details):

        if not kpi_details[Const.CONDITION]:
            return True
        elif kpi_details[Const.CONDITION] and kpi_details[Const.CONDITION] in self.red_score_condition:
            return True
        else:
            return False

    def _update_kpi_result(self, kpi, score):
        if isinstance(score, unicode):
            score = float(score)
        if isinstance(score, basestring):
            score = float(score)

        self.score += score
        if score > 0:
            self.red_score_condition.append(kpi)
        self.common_db.write_to_db_result(self.common_db.get_kpi_fk_by_kpi_name(kpi, self.common_db.LEVEL2),
                                          score=score,
                                          level=self.common_db.LEVEL2
                                          )

        self.common_db_integ.write_to_db_result(self.common_db_integ.get_kpi_fk_by_kpi_name(kpi, self.common_db.LEVEL3),
                                                score=score,
                                                level=self.common_db.LEVEL3
                                                )

    def _check_SKU_exculsion(self, kpi_details):
        filtered_template = self.exculsion_sheet[
            self.exculsion_sheet[Const.KPI_NAME] == kpi_details[Const.KPI_NAME].iloc[0]]
        scif_filter = Converters.get_scif_filter(filtered_template.iloc[0])
        return True if self.tools.calculate_availability(**scif_filter) > 0 else False

    def _check_survey_answer(self, kpi_details):
        if kpi_details['Required Answer']:
            return self.survey_handler.check_survey_answer(
                ('question_text', kpi_details[Const.SURVEY_SHEET_QUESTION_TEXT_FIELD]),
                kpi_details[Const.SURVEY_SHEET_ANSWER_FIELD], kpi_details['Required Answer'])
        else:
            return self.survey_handler.check_survey_answer(
                ('question_text', kpi_details[Const.SURVEY_SHEET_QUESTION_TEXT_FIELD]),
                kpi_details[Const.SURVEY_SHEET_ANSWER_FIELD])

    def _check_SOS(self, filtered_template):

        sos_filter = Converters.get_sos_filter(filtered_template)
        scif_filter = Converters.get_denominator_filter(filtered_template)

        if filtered_template[Const.STORE_ATT15] == 'DP' and scif_filter.get('att2'):
            scif_filter.pop('att2')

        scenes_sos_ratios = self.sos.calculate_sos_facing_by_scene(sos_filters=sos_filter, **scif_filter)

        dr_ppr_scenes_sos_ratios = {}
        # TODO check if this change is still needed
        if self._check_store_attribute(Const.STORE_ATT15, filtered_template):
            sos_filter['att2'] = filtered_template[Const.TRADEMARK].split(',')
            scif_filter['att2'] = filtered_template[Const.TRADEMARK].split(',')
            if sos_filter.get('manufacturer_name'):
                sos_filter.pop('manufacturer_name')

            if scif_filter.get('manufacturer_name'):
                scif_filter.pop('manufacturer_name')
            dr_ppr_scenes_sos_ratios = self.sos.calculate_sos_facing_by_scene(sos_filters=sos_filter, **scif_filter)

        if dr_ppr_scenes_sos_ratios:
            scenes_sos_ratios = self.merge_two_dicts(scenes_sos_ratios, dr_ppr_scenes_sos_ratios)

        if scenes_sos_ratios:
            scenes_sos_ratios = {k: v for k, v in scenes_sos_ratios.iteritems() if v}
        else:
            return False

        passed_scenes = []
        if scenes_sos_ratios:
            for scene, sos_ratio in scenes_sos_ratios.iteritems():
                if sos_ratio >= float(filtered_template[Const.TARGET]) / 100:
                    Log.info('KPI: {} sos value for scene: {} meets the target. ration is : {}'.format(
                        filtered_template[Const.KPI_NAME],
                        scene,
                        sos_ratio))
                    passed_scenes.append(scene)

            return passed_scenes if passed_scenes else None
        else:
            Log.info('KPI: {} didnt sound sos value for scene: {} meets the target. target is : {}'.format(
                filtered_template[Const.KPI_NAME],
                scenes_sos_ratios,
                float(
                    filtered_template[
                        Const.TARGET]) / 100))
            return None

    def _check_SOS_WITH_DP(self, filtered_template):
        trademark_filter = {}
        dp_filter = {}
        trademark_product_fk_list = []
        dp_product_fk_list = []
        sos_product_fk_list = []

        sos_static_filter, sos_entity_type_filter = Converters.get_sos_filter(filtered_template)
        denominator_filters = Converters.get_denominator_filter(filtered_template)

        # Trademark (att2)
        if sos_static_filter.get('att2'):
            trademark_filter = {'att2': sos_static_filter.pop('att2')}
        if trademark_filter:
            trademark_product_fk_list = self.scif[self.tools.get_filter_condition(self.scif, **trademark_filter)]['product_fk'].tolist()

        # Trademark DP
        if filtered_template[Const.STORE_ATT15] == 'DP' and filtered_template[Const.STORE_ATT_CONDITIONAL]:
            dp_filter = {'att2': filtered_template[Const.STORE_ATT_CONDITIONAL].split(',')}
        if dp_filter:
            dp_product_fk_list = self.scif[self.tools.get_filter_condition(self.scif, **dp_filter)]['product_fk'].tolist()

        # Entity Type & Value Numerator
        if sos_entity_type_filter:
            sos_product_fk_list = self.scif[self.tools.get_filter_condition(self.scif, **sos_entity_type_filter)]['product_fk'].tolist()

        numerator_filters = {'product_fk': trademark_product_fk_list + dp_product_fk_list + sos_product_fk_list}
        numerator_filters.update(sos_static_filter)

        if self._check_store_attribute(Const.STORE_ATT15, filtered_template):
            if numerator_filters['product_fk']:
                scenes_sos_ratios = self.sos.calculate_sos_facing_by_scene(sos_filters=numerator_filters,
                                                                           **denominator_filters)
            else:
                scenes_sos_ratios = {}
        else:
            if numerator_filters['product_fk']:
                scenes_sos_ratios = self.sos.calculate_sos_facing_by_scene(sos_filters=numerator_filters,
                                                                           **denominator_filters)
            else:
                scenes_sos_ratios = {}
        if scenes_sos_ratios:
            scenes_sos_ratios = {k: v for k, v in scenes_sos_ratios.iteritems() if v}
        else:
            return False

        passed_scenes = []
        if scenes_sos_ratios:
            for scene, sos_ratio in scenes_sos_ratios.iteritems():
                if sos_ratio >= float(filtered_template[Const.TARGET]) / 100:
                    Log.info('KPI: {} sos value for scene: {} meets the target. ration is : {}'.format(
                        filtered_template[Const.KPI_NAME],
                        scene,
                        sos_ratio))
                    passed_scenes.append(scene)

            return passed_scenes if passed_scenes else None
        else:
            Log.info('KPI: {} didnt sound sos value for scene: {} meets the target. target is : {}'.format(
                filtered_template[Const.KPI_NAME],
                scenes_sos_ratios,
                float(
                    filtered_template[
                        Const.TARGET]) / 100))
            return None

    def _check_incremental_kpi(self, kpi_name, scene_list, incremental_kpi):
        num_of_incrementals = len(incremental_kpi)

        for inc_kpi in incremental_kpi:
            if kpi_name == inc_kpi.strip():
                Log.info('kpi_name:False kpi_name and incremental_kpi have same name ')
                return False

            # get KPI details
            kpi_details = self.kpi_template[self.kpi_template[Const.KPI_NAME] == inc_kpi.strip()]
            if kpi_details[Const.SHEET].iloc[0] in (Const.SKU_AVAILABILITY_SHEET_NAME, Const.SOS_NAME):

                if kpi_details[Const.SHEET].iloc[0] == Const.SKU_AVAILABILITY_SHEET_NAME:
                    incremental_scene = self.calculate_red_score_availability(kpi_details)

                if kpi_details[Const.SHEET].iloc[0] == Const.SOS_NAME:
                    incremental_scene = self.calculate_SOS(kpi_details)

                if incremental_scene:
                    if incremental_scene[0] in scene_list:
                        scene_list.remove(incremental_scene[0])

                        # # check 1: is there more then 1 scene in the incremental per KPI
                        # if len(incremental_scene) > num_of_incrementals or len(incremental_scene) < 1:
                        #     Log.info('incremental check:True there is no other scene in incremental KPI'
                        #              ' or there more then one KPI:{}'.format(inc_kpi))
                        #
                        #     return True
                        # # check 2: is the single scene is same as in kpi
                        # elif len(incremental_scene) == 1:
                        #     if incremental_scene[0] in scene_list :
                        #         Log.info('incremental check:False there is same scene in incremental KPI.KPI:{}'.format(inc_kpi))
                        #         return False
                        #     else:
                        #         Log.info('incremental check:True there is no same scene in incremental KPI.KPI:{}'.format(inc_kpi))
                        #         return True
                        # else:
                        #     Log.info('incremental check:False no reason.KPI:{}'.format(inc_kpi))
                        #     return False


                        # else:
                        #     Log.info('check_incremental_kpi does supports only SKU_AVAILABILITY KPI')
                        #     return False

        return True if len(scene_list) > 0 else False

    def calculate_SOS(self, kpi_details):

        filtered_template = self.SOS_sheet[self.SOS_sheet[Const.KPI_NAME] == kpi_details[Const.KPI_NAME].iloc[0]]
        total_atomic_kpi = len(filtered_template)
        total_passed_atomic_kpi = 0
        passed_scene = []
        atomic_kpi_score = 0

        if total_atomic_kpi > 0:
            for i in xrange(total_atomic_kpi):
                kpi_name = filtered_template.iloc[i][Const.KPI_NAME]

                passed_scene_list = self._check_SOS(filtered_template.iloc[i])
                if not passed_scene_list:
                    atomic_kpi_score = 0

                else:
                    # check INCREMENTAL - check that KPI passed scene does not exits in incremental KPI
                    if kpi_details.iloc[0][Const.INCREMENTAL]:
                        inc_result = self._check_incremental_kpi(kpi_name, passed_scene_list,
                                                                 kpi_details.iloc[0][Const.INCREMENTAL].split(','))
                        if inc_result:
                            # KPI succeeded
                            atomic_kpi_score = 1
                            total_passed_atomic_kpi += 1
                            passed_scene += passed_scene_list
                        elif inc_result is False:
                            atomic_kpi_score = 0

                    else:
                        # KPI succeeded
                        atomic_kpi_score = 1
                        total_passed_atomic_kpi += 1
                        passed_scene += passed_scene_list

                        # write result to DB
                    atomic_kpi__fk = self.common_db.get_kpi_fk_by_kpi_name(kpi_name, self.common_db.LEVEL3)
                    result_dict = {'display_text': kpi_details.iloc[0]['Display Text']}
                    result_dict = self.common_db.create_basic_dict(atomic_kpi__fk, **result_dict)
                    self.common_db.write_to_db_result(atomic_kpi__fk, self.common_db.LEVEL3, atomic_kpi_score,
                                                      **result_dict)

                    return passed_scene if total_passed_atomic_kpi == total_atomic_kpi else False
        else:
            return None

    def calculate_SOS_WITH_DP(self, kpi_details):

        filtered_template = self.SOS_WITH_DP_sheet[self.SOS_WITH_DP_sheet[Const.KPI_NAME] == kpi_details[Const.KPI_NAME].iloc[0]]
        total_atomic_kpi = len(filtered_template)
        total_passed_atomic_kpi = 0
        passed_scene = []
        atomic_kpi_score = 0

        if total_atomic_kpi > 0:
            for i in xrange(total_atomic_kpi):
                kpi_name = filtered_template.iloc[i][Const.KPI_NAME]

                passed_scene_list = self._check_SOS_WITH_DP(filtered_template.iloc[i])
                if not passed_scene_list:
                    atomic_kpi_score = 0

                else:
                    # check INCREMENTAL - check that KPI passed scene does not exits in incremental KPI
                    if kpi_details.iloc[0][Const.INCREMENTAL]:
                        inc_result = self._check_incremental_kpi(kpi_name, passed_scene_list,
                                                                 kpi_details.iloc[0][Const.INCREMENTAL].split(','))
                        if inc_result:
                            # KPI succeeded
                            atomic_kpi_score = 1
                            total_passed_atomic_kpi += 1
                            passed_scene += passed_scene_list
                        elif inc_result is False:
                            atomic_kpi_score = 0

                    else:
                        # KPI succeeded
                        atomic_kpi_score = 1
                        total_passed_atomic_kpi += 1
                        passed_scene += passed_scene_list

                        # write result to DB
                    atomic_kpi__fk = self.common_db.get_kpi_fk_by_kpi_name(kpi_name, self.common_db.LEVEL3)

                    result_dict = {'display_text': kpi_details.iloc[0]['Display Text']}
                    result_dict = self.common_db.create_basic_dict(atomic_kpi__fk, **result_dict)
                    self.common_db.write_to_db_result(atomic_kpi__fk, self.common_db.LEVEL3, atomic_kpi_score,
                                                      **result_dict)

                    return passed_scene if total_passed_atomic_kpi == total_atomic_kpi else False
        else:
            return None

    def calculate_red_score_survey(self, kpi_details):
        filtered_template = self.survey_template[
            self.survey_template[Const.KPI_NAME] == kpi_details[Const.KPI_NAME].iloc[0]]
        total_atomic_kpi = len(filtered_template)
        total_passed_atomic_kpi = 0

        if kpi_details[Const.TEST_GROUP_CONDITION].iloc[0] == Const.ALL_PASSED:
            test_group_condition = total_atomic_kpi
        else:
            test_group_condition = int(kpi_details[Const.TEST_GROUP_CONDITION].iloc[0])

        if total_atomic_kpi > 0:
            for i in xrange(total_atomic_kpi):
                kpi_name = filtered_template.iloc[i][Const.KPI_NAME]
                if self._check_survey_answer(filtered_template.iloc[i]):
                    # KPI succeeded
                    atomic_kpi_score = 1
                    total_passed_atomic_kpi += 1
                else:
                    atomic_kpi_score = 0


                    # write result to DB
                atomic_kpi__fk = self.common_db.get_kpi_fk_by_kpi_name(kpi_name, self.common_db.LEVEL3)
                result_dict = {'display_text': kpi_details.iloc[0]['Display Text']}
                result_dict = self.common_db.create_basic_dict(atomic_kpi__fk, **result_dict)
                self.common_db.write_to_db_result(atomic_kpi__fk, self.common_db.LEVEL3, atomic_kpi_score,
                                                  **result_dict)

                # self.common_db.write_to_db_result(self.common_db.get_kpi_fk_by_kpi_name(kpi_name,self.common_db.LEVEL3),
                #                                   score=atomic_kpi_score,
                #                                   level=self.common_db.LEVEL3)

            return True if total_passed_atomic_kpi >= test_group_condition else False
        else:
            return False

    def calculate_red_score_availability(self, kpi_details):
        filtered_template = self.sku_availability_template[
            self.sku_availability_template[Const.KPI_NAME] == kpi_details[Const.KPI_NAME].iloc[0]]
        total_atomic_kpi = len(filtered_template)
        passed_scene = []

        if kpi_details[Const.TEST_GROUP_CONDITION].iloc[0] == Const.ALL_PASSED:
            test_group_condition = total_atomic_kpi
        else:
            test_group_condition = int(kpi_details[Const.TEST_GROUP_CONDITION].iloc[0])

        total_passed_atomic_kpi = 0
        if total_atomic_kpi > 0:
            for i in xrange(total_atomic_kpi):
                kpi_name = filtered_template.iloc[i][Const.KPI_NAME]

                # attribute 15 check
                if filtered_template.iloc[i][Const.STORE_ATT15] \
                        and self._check_store_attribute(Const.STORE_ATT15, filtered_template.iloc[i]):
                    total_passed_atomic_kpi += 1
                    continue

                # create filter
                scif_filter = Converters.get_scif_filter(filtered_template.iloc[i])

                # #TODO remove when value is in scif
                # if filtered_template.iloc[i][Const.PACKAGE_TYPE]:
                #     # att3_filter = {'product_fk':
                #     #                self._get_product_with_att3(filtered_template.iloc[i][Const.PACKAGE_TYPE])}
                #     att3_filter = {'att3': filtered_template.iloc[i][Const.PACKAGE_TYPE]}
                #     scif_filter.update(att3_filter)

                # test
                availability_df = self.availability.calculate_availability_by_scene(**scif_filter)
                brand_count = len(self.availability.calculate_brand_availability_by_scene(**scif_filter))

                passed_scene_list = availability_df.index.tolist()
                total_facings = availability_df['facings'].sum()
                brand_target = filtered_template.iloc[i][Const.BRAND_TARGET]
                if (total_facings < int(float(filtered_template.iloc[i][Const.TARGET2]))) or \
                        (brand_count < int(float(brand_target)) if brand_target else 0):
                    # KPI failed
                    atomic_kpi_score = 0
                else:
                    # check INCREMENTAL - check that KPI passed scene does not exits in incremental KPI
                    if kpi_details.iloc[0][Const.INCREMENTAL] and self._check_incremental_kpi(kpi_name,
                                                                                              passed_scene_list,
                                                                                              kpi_details.iloc[0][
                                                                                                  Const.INCREMENTAL].split(
                                                                                                  ',')):

                        # KPI succeeded
                        atomic_kpi_score = 1
                        total_passed_atomic_kpi += 1
                        passed_scene = passed_scene + passed_scene_list
                    elif kpi_details.iloc[0][Const.INCREMENTAL] and self._check_incremental_kpi(kpi_name,
                                                                                                passed_scene_list,
                                                                                                kpi_details.iloc[0][
                                                                                                    Const.INCREMENTAL].split(
                                                                                                    ',')) is False:
                        atomic_kpi_score = 0

                    else:
                        # KPI succeeded
                        atomic_kpi_score = 1
                        total_passed_atomic_kpi += 1
                        passed_scene = passed_scene + passed_scene_list

                # write result to DB
                atomic_kpi__fk = self.common_db.get_kpi_fk_by_kpi_name(kpi_name, self.common_db.LEVEL3)
                result_dict = {'display_text': kpi_details.iloc[0]['Display Text']}
                result_dict = self.common_db.create_basic_dict(atomic_kpi__fk, **result_dict)
                self.common_db.write_to_db_result(atomic_kpi__fk, self.common_db.LEVEL3, atomic_kpi_score,
                                                  **result_dict)

                # self.common_db.write_to_db_result(self.common_db.get_kpi_fk_by_kpi_name(kpi_name,self.common_db.LEVEL3),
                #                                   score=atomic_kpi_score,
                #                                   level=self.common_db.LEVEL3)

            return passed_scene if total_passed_atomic_kpi >= test_group_condition else None
        else:
            return None

    def calculate_scene_type(self, kpi_details):
        # get data template
        kpi_name = kpi_details[Const.KPI_NAME].iloc[0]
        filtered_templates = self.scene_type_sheet[self.scene_type_sheet[Const.KPI_NAME] == kpi_name]
        total_atomic_kpi = len(filtered_templates)
        total_pass_atomic_kpi = 0

        if kpi_details[Const.TEST_GROUP_CONDITION].iloc[0] == Const.ALL_PASSED:
            test_group_condition = total_atomic_kpi
        else:
            test_group_condition = int(kpi_details[Const.TEST_GROUP_CONDITION].iloc[0])

        if total_atomic_kpi > 0:
            for i in xrange(total_atomic_kpi):
                atomic_kpi_template = filtered_templates.iloc[i]
                kpi_filter = self._get_scene_type_scif_filter(atomic_kpi_template)

                if self.tools.calculate_number_of_scenes(**kpi_filter) >= 1:
                    total_pass_atomic_kpi += 1

        Log.info('session {}, KPI {} - scene_type - total passed {} out of '.format(self.session_uid,
                                                                                    kpi_name,
                                                                                    total_pass_atomic_kpi,
                                                                                    test_group_condition))
        kpi_result = True if total_pass_atomic_kpi >= test_group_condition else False

        if kpi_result:
            self.red_score_condition.append(kpi_name)

            atomic_kpi__fk = self.common_db.get_kpi_fk_by_kpi_name(kpi_name, self.common_db.LEVEL3)
            result_dict = {'display_text': kpi_details.iloc[0]['Display Text']}
            result_dict = self.common_db.create_basic_dict(atomic_kpi__fk, **result_dict)
            self.common_db.write_to_db_result(atomic_kpi__fk, self.common_db.LEVEL3, 1, **result_dict)
            #
            # self.common_db.write_to_db_result(self.common_db.get_kpi_fk_by_kpi_name(kpi_name,self.common_db.LEVEL3),
            #                                   score=1,
            #                                   level=self.common_db.LEVEL3)

        return kpi_result

    def calculate_red_score(self):
        # pull RED SCORE KPI
        kpi_list = self.kpi_template[self.kpi_template[Const.KPI_GROUP] == self.RED_SCORE][
            Const.KPI_NAME].unique().tolist()

        for kpi in kpi_list:
            Log.debug('session {}, KPI {} starting calculation '.format(self.session_uid, kpi))
            # get KPI details
            kpi_details = self.kpi_template[
                (self.kpi_template[Const.KPI_GROUP] == self.RED_SCORE) & (self.kpi_template[Const.KPI_NAME] == kpi)]
            max_score = self.max_scores_dict[kpi_details.iloc[0][Const.STORE_TYPE]]

            # basic check
            if not self.checks.check_store(kpi_details.iloc[0]):
                Log.debug('session {}, KPI {} store does not match'.format(self.session_uid, kpi))
                Log.debug('session {}, KPI {} store does not match'.format(self.session_uid, kpi))
                # move to next KPI
                continue

            # check Exclusion Sheet
            if kpi_details[Const.SHEET].iloc[0] == Const.EXCLUSION_SHEET and self._check_SKU_exculsion:
                Log.debug('session {}, KPI {} found exclusion score = 0'.format(self.session_uid, kpi))
                self._update_kpi_result(kpi, 0)
                continue

            if self._check_kpi_condition(kpi_details.iloc[0]):  # check if prerequisites KPI fulfilled
                Log.debug('session {}, KPI {} previous condition are ok '.format(self.session_uid, kpi))
                # SOS all passed
                if kpi_details[Const.SHEET].iloc[0] == Const.SOS_NAME:
                    if self.calculate_SOS(kpi_details):
                        # update to db + calc red score
                        kpi_score = (float(kpi_details[Const.WEIGHT].iloc[0]) / float(max_score)) * 100
                        Log.info('session {}, KPI {} , score {} '.format(self.session_uid, kpi, kpi_score))
                    else:
                        kpi_score = 0

                    self._update_kpi_result(kpi, kpi_score)

                # SOS with dp all passed
                if kpi_details[Const.SHEET].iloc[0] == Const.SOS_WITH_DP_NAME:
                    if self.calculate_SOS_WITH_DP(kpi_details):
                        # update to db + calc red score
                        kpi_score = (float(kpi_details[Const.WEIGHT].iloc[0]) / float(max_score)) * 100
                        Log.info('session {}, KPI {} , score {} '.format(self.session_uid, kpi, kpi_score))
                    else:
                        kpi_score = 0

                    self._update_kpi_result(kpi, kpi_score)

                # scene availability
                if kpi_details[Const.SHEET].iloc[0] == Const.SCENE_AVAILABILITY_SHEET_NAME:
                    if self.calculate_scene_type(kpi_details):
                        # update to db + calc red score
                        kpi_score = (float(kpi_details[Const.WEIGHT].iloc[0]) / float(max_score)) * 100
                        Log.debug('session {}, KPI {} , score {} '.format(self.session_uid, kpi, kpi_score))
                    else:
                        kpi_score = 0

                    self._update_kpi_result(kpi, kpi_score)

                # SKU_Availability
                if kpi_details[Const.SHEET].iloc[0] == Const.SKU_AVAILABILITY_SHEET_NAME:

                    passed_scene = self.calculate_red_score_availability(kpi_details)

                    if passed_scene:
                        # update to db + calc red score
                        kpi_score = (float(kpi_details[Const.WEIGHT].iloc[0]) / float(max_score)) * 100
                        Log.info('session {}, KPI {} , score {} '.format(self.session_uid, kpi, kpi_score))
                    else:
                        kpi_score = 0

                    self._update_kpi_result(kpi, kpi_score)

                # Survey
                if kpi_details[Const.SHEET].iloc[0] == Const.SURVEY_SHEET_NAME:
                    if self.calculate_red_score_survey(kpi_details):
                        # update to db + calc red score
                        kpi_score = (float(kpi_details[Const.WEIGHT].iloc[0]) / float(max_score)) * 100
                        Log.info('session {}, KPI {} , score {} '.format(self.session_uid, kpi, kpi_score))
                    else:
                        kpi_score = 0

                    self._update_kpi_result(kpi, kpi_score)

            else:
                # kpi condition not fulfilled
                self._update_kpi_result(kpi, 0)

        # build red score sql
        self.common_db.write_to_db_result(self.common_db.get_kpi_fk_by_kpi_name(self.RED_SCORE, self.common_db.LEVEL1),
                                          score=self.score,
                                          level=self.common_db.LEVEL1
                                          )

        self.common_db_integ.write_to_db_result(
            self.common_db_integ.get_kpi_fk_by_kpi_name(self.RED_SCORE_INTEG, self.common_db.LEVEL1),
            score=self.score,
            level=self.common_db.LEVEL1
            )

        # commit all results
        self.common_db.commit_results_data_without_delete()
        self.common_db_integ.commit_results_data_without_delete()
        Log.info('RED_SCORE result for session {} : {}'.format(self.session_uid, self.score))

        return self.score
