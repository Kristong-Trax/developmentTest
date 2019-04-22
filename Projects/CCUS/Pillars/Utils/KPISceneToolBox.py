from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCUS.Pillars.Utils.Const import Const

import os
import pandas as pd

__author__ = 'Jasmine'


class PillarsSceneToolBox:
    PROGRAM_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                         'Data', 'Template.xlsx')

    def __init__(self, data_provider, common, common_old):
        self.data_provider = data_provider
        self.common = common
        self.common_old = common_old
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
        # self.kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POC)
        self.poc_number = 1

    def is_scene_belong_to_program(self):
        relevant_programs = self.get_programs()
        for i in xrange(len(relevant_programs)):
            current_program_data = relevant_programs.iloc[i]
            program_as_brand = current_program_data[Const.PROGRAM_NAME_BY_BRAND]
            program_as_template = current_program_data[Const.PROGRAM_NAME_BY_TEMPLATE]
            survey_question_for_program = current_program_data[Const.PROGRAM_NAME_BY_SURVEY_QUESTION]
            program_as_survey_answer = current_program_data[Const.PROGRAM_NAME_BY_SURVEY_ANSWER]
            display_id = current_program_data[Const.PROGRAM_NAME_BY_DISPLAY_ID]
            score = 0

            if self.found_program_products_by_brand(program_as_brand) \
                    or self.found_scene_program_by_quri(program_as_template) \
                    or self.found_scene_program_by_survey(survey_question_for_program, program_as_survey_answer) \
                    or self.found_scene_program_by_display(display_id):
                score = 1
            print score

    #           write to db scene and score

    def get_programs(self):
        programs = pd.read_excel(self.PROGRAM_TEMPLATE_PATH)

        # Get only relevant programs to check
        relevant_programs = programs.loc[(programs['start_date'].dt.date <= self.visit_date) &
                                         (programs['end_date'].dt.date >= self.visit_date)]
        relevant_programs = relevant_programs[Const.PROGRAM_NAME_FIELD].unique()


        return relevant_programs

    def update_old_kpi_static(self, atomic_list):
        old_kpis = self.common_old.get_kpi_static_data()
        old_kpis = old_kpis.loc[old_kpis['kpi_set_name'] == Const.KPI_SET]
        for atomic in atomic_list:
            pass


    def found_program_products_by_brand(self, brand_name):
        # checks if the scene's program was discovered by trax according to its products
        if pd.isnull(brand_name):
            return False
        pos_products_in_brand = self.all_products[(self.all_products['product_type'] == 'POS') &
                                                  (self.all_products['brand_name'] == brand_name)
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

        if pd.isnull(survey_question) or pd.isnull(survey_question):
            return False
        pass

    def found_scene_program_by_display(self, display_id):
        # checks if the scene's program was discovered by scene recognition

        if pd.isnull(display_id):
            return False

        if self.data_provider.match_display_in_scene.empty:
            return False
        return display_id in self.data_provider.match_display_in_scene['display_fk']
