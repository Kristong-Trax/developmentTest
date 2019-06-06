from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCUS.Pillars.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


import os
import pandas as pd

__author__ = 'Jasmine'

BINARY = 2

class PillarsSceneToolBox:
    PROGRAM_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'Data', 'CCUS_Templatev2.2.xlsx')
    BITWISE_RECOGNIZER_SIZE = 6
    RECOGNIZED_BY_POS = BITWISE_RECOGNIZER_SIZE - 1
    RECOGNIZED_BY_SCENE_RECOGNITION = BITWISE_RECOGNIZER_SIZE - 2
    RECOGNIZED_BY_QURI = BITWISE_RECOGNIZER_SIZE - 3
    RECOGNIZED_BY_SURVEY = BITWISE_RECOGNIZER_SIZE - 4

    def __init__(self, data_provider,output, common):
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        empties = self.all_products[self.all_products['product_type'] == 'Empty']['product_fk'].unique().tolist()
        self.match_product_in_scene = self.match_product_in_scene[
            ~(self.match_product_in_scene['product_fk'].isin(empties))]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.template_fk = self.templates['template_fk'].iloc[0]
        self.scene_id = self.scene_info['scene_fk'][0]
        self.store_id = self.data_provider[Data.STORE_INFO]['store_fk'][0]
        # self.kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POC)
        self.all_brand = self.all_products[['brand_name', 'brand_fk']].drop_duplicates().set_index(u'brand_name')
        self.displays_in_scene = self.data_provider.match_display_in_scene
        self.ps_data_provider = PsDataProvider(self.data_provider, output)

        # bit-like sequence to symoblize recognizing methods. Each 'bit' symbolize a recognition method.
        self.bitwise_for_program_identifier_as_list = list("0" * self.BITWISE_RECOGNIZER_SIZE)

    def is_scene_belong_to_program(self):
        # Get template (from file or from external targets)
        relevant_programs = self.get_programs()

        for i in xrange(len(relevant_programs)):

            # Get data for program from template:
            current_program_data = relevant_programs.iloc[i]
            program_name = current_program_data[Const.PROGRAM_NAME_FIELD]  # assumed to always be brand name!
            program_brand_name_fk = self.get_brand_fk_from_name(program_name)
            program_as_brand = current_program_data[Const.PROGRAM_NAME_BY_BRAND]
            program_as_brand_fk = self.get_brand_fk_from_name(program_as_brand)
            program_as_display_brand = current_program_data[Const.PROGRAM_NAME_BY_DISPLAY]
            program_as_template = current_program_data[Const.PROGRAM_NAME_BY_TEMPLATE]
            survey_question_for_program = current_program_data[Const.PROGRAM_NAME_BY_SURVEY_QUESTION]
            program_as_survey_answer = current_program_data[Const.PROGRAM_NAME_BY_SURVEY_ANSWER]
            score = 0

            # Checks if the scene was recognized as relevant program in one of possible recognition options:
            self.bitwise_for_program_identifier_as_list[self.RECOGNIZED_BY_POS] = \
                1 if self.found_program_products_by_brand(program_as_brand_fk) else 0

            self.bitwise_for_program_identifier_as_list[self.RECOGNIZED_BY_SCENE_RECOGNITION] = \
                1 if self.found_scene_program_by_display_brand(program_as_display_brand) else 0

            self.bitwise_for_program_identifier_as_list[self.RECOGNIZED_BY_QURI] = \
                1 if self.found_scene_program_by_quri(program_as_template) else 0

            self.bitwise_for_program_identifier_as_list[self.RECOGNIZED_BY_SURVEY] = \
                1 if self.found_scene_program_by_survey(survey_question_for_program, program_as_survey_answer) else 0

            # convert list of bits to a string in order to convert to decimal in results:
            bitwise_for_program_identifier_as_str = ''.join(map(str, self.bitwise_for_program_identifier_as_list))

            # convert string of binary-like to decimal value
            method_recognized_in_bitwise = int(bitwise_for_program_identifier_as_str, BINARY)

            score = 1 if method_recognized_in_bitwise > 0 else 0

            scene_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name=Const.SCENE_KPI_NAME)
            self.common.write_to_db_result(fk=scene_kpi_fk, numerator_id=program_brand_name_fk,
                                           result=method_recognized_in_bitwise, score=score, by_scene=True,
                                           denominator_id=self.store_id)

    def get_programs(self, template=False):
        """
        This function gets the relevant programs from template/ external targets list.
        :param template: if True takes the template from Data folder in project.
                Else, takes the kpi's external targets.
        :return:
        """
        if template:
            programs = pd.read_excel(self.PROGRAM_TEMPLATE_PATH)
        else:
            programs = self.ps_data_provider.get_kpi_external_targets(["Pillars Programs KPI"])

        if programs.empty:
            return programs

        # Get only relevant programs to check
        relevant_programs = programs.loc[(programs['start_date'].dt.date <= self.visit_date) &
                                         (programs['end_date'].dt.date >= self.visit_date)]

        return relevant_programs

    def get_brand_fk_from_name(self, brand_name):
        if pd.isnull(brand_name):
            return
        fk = self.all_brand.loc[brand_name]
        if not fk.empty:
            fk = fk.values[0]
        else:
            fk = None
        return fk


    def found_program_products_by_brand(self, brand_fk=None, brand_name=None):
        """
        This function can get brand either by fk or by name, with the assumption that in the
        'customer' template there is brand name and in the db targets there is pk.
        If none of the option was used, return False.
        Otherwise return if there were product in this scene with the brand given.
        """
        # checks if the scene's program was discovered by trax according to brand's recognized products
        if pd.isnull(brand_name) and pd.isnull(brand_fk):
            return False

        brand_id = 'brand_fk' if brand_fk else 'brand_name'
        brand_value = brand_fk if brand_fk else brand_name

        pos_products_in_brand = self.all_products[(self.all_products['product_type'] == 'POS') &
                                                  (self.all_products[brand_id] == brand_value)
                                                  ]['product_fk'].unique().tolist()
        program_products_in_scene = self.match_product_in_scene[
            (self.match_product_in_scene['product_fk'].isin(pos_products_in_brand))]
        return len(program_products_in_scene) > 0

    def found_scene_program_by_quri(self, template_name):
        # checks if the scene's program was discovered by quri according to the template name

        if pd.isnull(template_name):
            return False

        if self.templates['template_fk'].empty:
            return False

        return template_name in self.templates['template_name'].values

    def found_scene_program_by_survey(self, survey_question, survey_answer):  # TODO- complete after survey created

        # Cannot complete until found relevant tables

        if pd.isnull(survey_question) or pd.isnull(survey_answer):
            return False

        pass

    def found_scene_program_by_display_brand(self, brand_name):
        # checks if the scene's program was discovered by scene recognition

        if pd.isnull(brand_name):
            return False

        if self.data_provider.match_display_in_scene.empty:
            return False
        display_in_brand = len(self.data_provider.match_display_in_scene.loc[
            self.data_provider.match_display_in_scene['display_brand_name'] == brand_name])

        return display_in_brand > 0

