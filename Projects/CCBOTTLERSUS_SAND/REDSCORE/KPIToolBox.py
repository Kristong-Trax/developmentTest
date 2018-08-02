import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS_SAND.REDSCORE.SceneKPIToolBox import CCBOTTLERSUS_SANDSceneRedToolBox
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPITemplateV3.xlsx')
SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SurveyTemplateV1.xlsx')


class CCBOTTLERSUS_SANDREDToolBox:

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.survey = Survey(self.data_provider, self.output)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.templates = {}
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.converters = self.templates[Const.CONVERTERS]
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.common = Common(self.data_provider)
        self.kpi_static_data_session = self.common.kpi_static_data
        main_template = self.templates[Const.KPIS]
        self.templates[Const.KPIS] = main_template[(main_template[Const.REGION] == self.region) &
                                                   (main_template[Const.STORE_TYPE] == self.store_type)]
        self.scene_calculator = CCBOTTLERSUS_SANDSceneRedToolBox(
            data_provider, output, self.templates, self.common, self)
        self.scenes_results = None
        self.session_results = pd.DataFrame(columns=Const.COLUMNS_OF_SESSION)
        self.all_results = pd.DataFrame(columns=Const.COLUMNS_OF_SCENE)
        self.used_scenes = []
        self.red_score = 0

# main functions:

    def main_calculation(self, *args, **kwargs):
        """
            :param kwargs: dict - kpi line from the template.
            the function gets the kpi (level 2) row, and calculates its children.
            :return: float - score of the kpi.
        """
        self.scenes_results = self.scene_calculator.main_calculation()
        main_template = self.templates[Const.KPIS]
        session_template = main_template[main_template[Const.SESSION_LEVEL] == Const.V]
        for i, main_line in session_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.filter_results()

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        target = main_line[Const.GROUP_TARGET]
        kpi_type = main_line[Const.SHEET]
        relevant_scif = self.scif
        parent = main_line[Const.CONDITION]
        if kpi_type == Const.SCENE_AVAILABILITY:
            result = self.calculate_scene_availability(main_line)
        else:
            scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
            if scene_types:
                relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            scene_groups = self.does_exist(main_line, Const.SCENE_TYPE_GROUP)
            if scene_groups:
                relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            isnt_dp = False
            if self.store_attr != Const.DP and main_line[Const.STORE_ATTRIBUTE] == Const.DP:
                isnt_dp = True
            relevant_template = self.templates[kpi_type]
            relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
            if target == Const.ALL:
                target = len(relevant_template)
            function = self.get_kpi_function(kpi_type)
            if main_line[Const.SAME_PACK] == Const.V:
                result = self.calculate_availability_with_same_pack(relevant_template, relevant_scif, isnt_dp)
            else:
                passed_counter = 0
                for i, kpi_line in relevant_template.iterrows():
                    answer = function(kpi_line, relevant_scif, isnt_dp)
                    if answer:
                        passed_counter += 1
                result = passed_counter >= target
        self.write_to_session_level(kpi_name=kpi_name, result=result, parent=parent)

# write in DF:

    def write_to_session_level(self, kpi_name, result=0, parent=""):
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result * 1}
        self.session_results = self.session_results.append(result_dict, ignore_index=True)

    def write_to_all_levels(self, kpi_name, result, display_text, weight, scene_fk=None):
        self.red_score += (weight / 100.0) * result
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result}
        if scene_fk:
            result_dict[Const.SCENE_FK] = scene_fk
            self.used_scenes.append(scene_fk)
        self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.write_to_db(kpi_name, result, display_text)

# survey:

    def calculate_survey_specific(self, kpi_line, relevant_scif, isnt_dp):
        question = kpi_line[Const.Q_TEXT]
        if not question:
            question_id = kpi_line[Const.Q_ID]
            if question_id == "":
                return False
            question = ('question_fk', int(question_id))
        answers = kpi_line[Const.ACCEPTED_ANSWER].split(',')
        for answer in answers:
            if self.survey.check_survey_answer(survey_text=question, target_answer=answer):
                return True
        return False

    # availability:

    def calculate_availability_with_same_pack(self, relevant_template, relevant_scif, isnt_dp):
        packages = None
        for i, kpi_line in relevant_template.iterrows():
            if isnt_dp and relevant_template[Const.MANUFACTURER] in Const.DP_MANU:
                continue
            filtered_scif = self.filter_scif_availability(kpi_line, relevant_scif)
            target = kpi_line[Const.TARGET]
            sizes = filtered_scif['size'].tolist()
            sub_packages_nums = filtered_scif['number_of_sub_packages'].tolist()
            cur_packages = set(zip(sizes, sub_packages_nums))
            if packages is None:
                packages = cur_packages
            else:
                packages = cur_packages & packages
                if len(packages) == 0:
                    return False
            if filtered_scif['facings'].sum() < target:
                return False
        return True

    def calculate_availability(self, kpi_line, relevant_scif, isnt_dp):
        if isnt_dp and kpi_line[Const.MANUFACTURER] in Const.DP_MANU:
            return True
        filtered_scif = self.filter_scif_availability(kpi_line, relevant_scif)
        target = kpi_line[Const.TARGET]
        return filtered_scif['facings'].sum() >= target

    def filter_scif_specific(self, relevant_scif, kpi_line, name_in_template, name_in_scif):
        values = self.does_exist(kpi_line, name_in_template)
        if values:
            return relevant_scif[relevant_scif[name_in_scif].isin(values)]
        return relevant_scif

    def filter_scif_availability(self, kpi_line, relevant_scif):
        names_of_columns = {
            Const.MANUFACTURER: "manufacturer_name",
            Const.BRAND: "brand_name",
            Const.TRADEMARK: "att2",
            Const.SIZE: "size",
            Const.NUM_SUB_PACKAGES: "number_of_sub_packages",
            Const.PREMIUM_SSD: "Premium SSD",
            Const.INNOVATION_BRAND: "Innovation Brand",
        }
        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(relevant_scif, kpi_line, name, names_of_columns[name])
        return relevant_scif

    # scene availability:

    def calculate_scene_availability(self, main_line):
        scene_type = main_line[Const.SCENE_TYPE]
        scene_group = main_line[Const.SCENE_TYPE_GROUP]
        if scene_type in self.scif['template_name'].unique().tolist() \
                and scene_group in self.scif['template_group'].unique().tolist():
            return True
        return False

    # SOS:

    def calculate_sos(self, kpi_line, relevant_scif, isnt_dp):
        kpi_name = kpi_line[Const.KPI_NAME]
        if kpi_line[Const.EXCLUSION_SHEET] == Const.V:
            exclusion_sheet = self.templates[Const.SKU_EXCLUSION]
            relevant_exclusions = exclusion_sheet[exclusion_sheet[Const.KPI_NAME] == kpi_name]
            for i, exc_line in relevant_exclusions.iterrows():
                relevant_scif = self.exclude_scif(exc_line, relevant_scif)
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1]
        relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        if kpi_line[Const.SSD_STILL] != "":
            relevant_scif = self.filter_by_type_value(relevant_scif, Const.SSD_STILL, kpi_line[Const.SSD_STILL])
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1]
        num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        if isnt_dp:
            num_scif = num_scif[~(num_scif['manufacturer_name'].isin(Const.DP_MANU))]
        target = float(kpi_line[Const.TARGET]) / 100
        return num_scif['facings'].sum() / relevant_scif['facings'].sum() >= target

    # SOS majority:

    def calculate_sos_maj(self, kpi_line, relevant_scif, isnt_dp):
        kpi_name = kpi_line[Const.KPI_NAME]
        if kpi_line[Const.EXCLUSION_SHEET] == Const.V:
            exclusion_sheet = self.templates[Const.SKU_EXCLUSION]
            relevant_exclusions = exclusion_sheet[exclusion_sheet[Const.KPI_NAME] == kpi_name]
            for i, exc_line in relevant_exclusions.iterrows():
                relevant_scif = self.exclude_scif(exc_line, relevant_scif)
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1]
        relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        den_type = kpi_line[Const.DEN_TYPES_2]
        den_value = kpi_line[Const.DEN_VALUES_2]
        relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        if kpi_line[Const.MAJ_DOM] == Const.MAJOR:
            answer = self.calculate_majority_part(kpi_line, relevant_scif, isnt_dp)
        elif kpi_line[Const.MAJ_DOM] == Const.DOMINANT:
            answer = self.calculate_dominant_part(kpi_line, relevant_scif, isnt_dp)
        else:
            answer = False
        return answer

    def calculate_majority_part(self, kpi_line, relevant_scif, isnt_dp):
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1]
        num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        num_type = kpi_line[Const.NUM_TYPES_2]
        num_value = kpi_line[Const.NUM_VALUES_2]
        num_scif = self.filter_by_type_value(num_scif, num_type, num_value)
        if isnt_dp:
            num_scif = num_scif[~(num_scif['manufacturer_name'].isin(Const.DP_MANU))]
        target = Const.MAJORITY_TARGET
        return num_scif['facings'].sum() / relevant_scif['facings'].sum() >= target

    def calculate_dominant_part(self, kpi_line, relevant_scif, isnt_dp):
        if isnt_dp:
            relevant_scif = relevant_scif[~(relevant_scif['manufacturer_name'].isin(Const.DP_MANU))]
        type_name = self.get_column_name(kpi_line[Const.NUM_TYPES_1], relevant_scif)
        values = kpi_line[Const.NUM_VALUES_1].split(', ')
        max_facings, needed_one = 0, 0
        values_type = relevant_scif[type_name].unique().tolist()
        if None in values_type:
            values_type.remove(None)
            current_sum = relevant_scif[relevant_scif[type_name].isnull()]['facings'].sum()
            if current_sum > max_facings:
                max_facings = current_sum
        for value in values_type:
            current_sum = relevant_scif[relevant_scif[type_name] == value]['facings'].sum()
            if current_sum > max_facings:
                max_facings = current_sum
            if value in values:
                needed_one += current_sum
        return needed_one >= max_facings

    # helpers:

    def get_column_name(self, field_name, df):
        if field_name in df.columns:
            return field_name
        if field_name.upper() in self.converters[Const.NAME_IN_TEMP].str.upper().tolist():
            field_name = self.converters[self.converters[Const.NAME_IN_TEMP].str.upper() == field_name.upper()][
                Const.NAME_IN_DB].iloc[0]
            return field_name
        return None

    def filter_by_type_value(self, relevant_scif, type_name, value):
        if type_name == "":
            return relevant_scif
        values = value.split(', ')
        new_type_name = self.get_column_name(type_name, relevant_scif)
        if not new_type_name:
            print "There is no field '{}'".format(type_name)
            return relevant_scif
        return relevant_scif[relevant_scif[new_type_name].isin(values)]

    @staticmethod
    def exclude_scif(exclude_line, relevant_scif):
        exclude_products = exclude_line[Const.PRODUCT_EAN].split(', ')
        return relevant_scif[~(relevant_scif['product_ean_code'].isin(exclude_products))]

    @staticmethod
    def does_exist(kpi_line, column_name):
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return cell.split(", ")
        return None

    def get_kpi_function(self, kpi_type):
        if kpi_type == Const.SURVEY:
            return self.calculate_survey_specific
        elif kpi_type == Const.AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == Const.SCENE_AVAILABILITY:
            return self.calculate_scene_availability
        elif kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.SOS_MAJOR:
            return self.calculate_sos_maj
        else:
            return None

    def filter_results(self):
        self.scenes_results.to_csv('scene {}.csv'.format(self.store_type))
        self.session_results.to_csv('session {}.csv'.format(self.store_type))
        main_template = self.templates[Const.KPIS]
        self.write_session_kpis(main_template)
        self.write_scene_kpis(main_template)
        self.write_condition_kpis(main_template)
        result_dict = {Const.KPI_NAME: 'red score', Const.RESULT: self.red_score}
        self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.all_results.to_csv('all {}.csv'.format(self.store_type))

    def write_session_kpis(self, main_template):
        session_template = main_template[(main_template[Const.SESSION_LEVEL] == Const.V) &
                                         (main_template[Const.CONDITION] == "")]
        for i, main_line in session_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            result = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            if result.empty:
                continue
            result = result.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_scene_kpis(self, main_template):
        scene_template = main_template[(main_template[Const.SESSION_LEVEL] != Const.V) &
                                       (main_template[Const.CONDITION] == "")]
        incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        while not incremental_template.empty:
            for i, main_line in incremental_template.iterrows():
                kpi_name = main_line[Const.KPI_NAME]
                kpi_results = self.scenes_results[(self.scenes_results[Const.KPI_NAME] == kpi_name) &
                                                  (~(self.scenes_results[Const.SCENE_FK].isin(self.used_scenes)))]
                true_results = kpi_results[kpi_results[Const.RESULT] > 0]
                increments = main_line[Const.INCREMENTAL]
                if ', ' in increments:
                    first_kpi = increments.split(', ')[0]
                    others = increments.replace(', '.format(first_kpi), '')
                    scene_template.loc[scene_template[Const.KPI_NAME] == first_kpi, Const.INCREMENTAL] = others
                if true_results.empty:
                    scene_template.loc[scene_template[Const.KPI_NAME] == kpi_name, Const.INCREMENTAL] = ""
                else:
                    true_results = true_results.sort_values(by=Const.RESULT, ascending=False)
                    display_text = main_line[Const.DISPLAY_TEXT]
                    weight = main_line[Const.WEIGHT]
                    scene_fk = true_results.iloc[0][Const.SCENE_FK]
                    self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.RESULT], display_text, weight, scene_fk=scene_fk)
                    scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
            incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            kpi_results = self.scenes_results[(self.scenes_results[Const.KPI_NAME] == kpi_name) &
                                              (~(self.scenes_results[Const.SCENE_FK].isin(self.used_scenes)))]
            true_results = kpi_results[kpi_results[Const.RESULT] > 0]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if true_results.empty:
                continue
            true_results = true_results.sort_values(by=Const.RESULT, ascending=False)
            scene_fk = true_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.RESULT], display_text, weight, scene_fk=scene_fk)
            scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            kpi_results = self.scenes_results[(self.scenes_results[Const.KPI_NAME] == kpi_name) &
                                              (~(self.scenes_results[Const.SCENE_FK].isin(self.used_scenes)))]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if kpi_results.empty:
                continue
            scene_fk = kpi_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, 0, display_text, weight, scene_fk=scene_fk)

    def write_condition_kpis(self, main_template):
        condition_template = main_template[main_template[Const.CONDITION] != '']
        for i, main_line in condition_template.iterrows():
            condition = main_line[Const.CONDITION]
            kpi_name = main_line[Const.KPI_NAME]
            if main_line[Const.SESSION_LEVEL] == Const.V:
                kpi_results = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            else:
                kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            condition_result = self.all_results[(self.all_results[Const.KPI_NAME] == condition) &
                                                (self.all_results[Const.RESULT] > 0)]
            if condition_result.empty:
                continue
            condition_result = condition_result.iloc[0]
            condition_scene = condition_result[Const.SCENE_FK]
            if condition_scene:
                results = kpi_results[kpi_results[Const.SCENE_FK] == condition_scene]
            else:
                results = kpi_results
            if results.empty:
                continue
            result = results.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            scene_fk = results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, result, display_text, weight, scene_fk=scene_fk)

    def write_to_db(self, kpi_name, result, display_text):
        result_dict = {'display_text': display_text}
        kpi_fk_3 = self.common.get_kpi_fk_by_kpi_name(kpi_name, self.common.LEVEL3)
        result_dict3 = self.common.create_basic_dict(kpi_fk_3, **result_dict)
        self.common.write_to_db_result(kpi_fk_3, self.common.LEVEL3, result, **result_dict3)
        # kpi_fk_2 = self.common.get_kpi_fk_by_kpi_name(kpi_name, self.common.LEVEL2)
        # result_dict2 = self.common.create_basic_dict(kpi_fk_2, **result_dict)
        # self.common.write_to_db_result(kpi_fk_2, self.common.LEVEL2, result, **result_dict2)
        self.common.write_to_db_result(self.common.get_kpi_fk_by_kpi_name(kpi_name, self.common.LEVEL2),
                                       score=result, level=self.common.LEVEL2)
        # self.common.write_to_db_result(self.common.get_kpi_fk_by_kpi_name(kpi_name, self.common.LEVEL3),
        #                                score=result, level=self.common.LEVEL3)
