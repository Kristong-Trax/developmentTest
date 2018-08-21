import os
from datetime import datetime
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.CMA.Const import Const
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS


__author__ = 'Uri'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KPITemplateV4.1.xlsx')
SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SurveyTemplateV2.xlsx')
############
STORE_TYPES = {
    "CR SOVI RED": "CR&LT",
    "DRUG SOVI RED": "Drug",
    "VALUE SOVI RED": "Value",
    "United Test - Value SOVI RED": "Value",
    "United Test - Drug SOVI RED": "Drug",
    "United Test - CR SOVI RED": "CR&LT",
    "FSOP - QSR": "QSR",
}
CMA_COMPLIANCE = 'CMA Compliance'


class CCBOTTLERSUSCMAToolBox:

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
        self.united_scenes = self.get_united_scenes() # we don't need to check scenes without United products
        self.survey = Survey(self.data_provider, self.output)
        self.sos = SOS(self.data_provider, self.output)
        self.templates = {}
        self.common_db = Common(self.data_provider, CMA_COMPLIANCE)
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        if self.store_type in STORE_TYPES: #####
            self.store_type = STORE_TYPES[self.store_type] ####
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.kpi_static_data = self.common_db.get_kpi_static_data()
        main_template = self.templates[Const.KPIS]
        self.templates[Const.KPIS] = main_template[(main_template[Const.REGION] == self.region) &
                                                   (main_template[Const.STORE_TYPE] == self.store_type)]
        self.used_scenes = []
        self.red_score = 0
        self.weight_factor = self.get_weight_factor()

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.choose_and_write_results()

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.SHEET]
        relevant_scif = self.scif[self.scif['scene_id'].isin(self.united_scenes)]
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        result = score = target = None
        general_filters = {}
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        scene_groups = self.does_exist(main_line, Const.SCENE_TYPE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            general_filters['template_group'] = scene_groups
        if kpi_type == Const.SOS:
            isnt_dp = True if self.store_attr != Const.DP and main_line[Const.STORE_ATTRIBUTE] == Const.DP else False
            relevant_template = self.templates[kpi_type]
            relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
            function = self.get_kpi_function(kpi_type)
            for i, kpi_line in relevant_template.iterrows():
                result, score, target = function(kpi_line, relevant_scif, isnt_dp, general_filters)
        else:
            pass
        self.write_to_all_levels(kpi_name=kpi_name, result=result, score=score, target=target)

    def calculate_manual_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        relevant_template = self.templates[Const.SURVEY]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        target = len(relevant_template) if main_line[Const.GROUP_TARGET] == Const.ALL \
            else main_line[Const.GROUP_TARGET]
        passed_counter = 0
        for i, kpi_line in relevant_template.iterrows():
            answer = self.calculate_survey_specific(kpi_line)
            if answer:
                passed_counter += 1
        result = passed_counter >= target
        self.write_to_session_level(kpi_name=kpi_name, result=result)

    # write in DF:

    def write_to_session_level(self, kpi_name, result=0):
        """
        Writes a result in the DF
        :param kpi_name: string
        :param result: boolean
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result * 1}
        self.session_results = self.session_results.append(result_dict, ignore_index=True)

    def write_to_all_levels(self, kpi_name, result, score, target=None, scene_fk=None, reuse_scene=False):
        """
        Writes the final result in the "all" DF, add the score to the red score and writes the KPI in the DB
        :param kpi_name: str
        :param result: int
        :param display_text: str
        :param weight: int/float
        :param scene_fk: for the scene's kpi
        :param reuse_scene: this kpi can use scenes that were used
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result, Const.SCORE: score, Const.THRESHOLD: target}
        if scene_fk:
            result_dict[Const.SCENE_FK] = scene_fk
            if not reuse_scene:
                self.used_scenes.append(scene_fk)
        self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.write_to_db(kpi_name, score, result=result, threshold=target)

    # survey:

    def calculate_survey_specific(self, kpi_line, relevant_scif=None, isnt_dp=None):
        """
        returns a survey line if True or False
        :param kpi_line: line from the survey sheet
        :param relevant_scif:
        :param isnt_dp:
        :return: True or False - if the question gets the needed answer
        """
        question = kpi_line[Const.Q_TEXT]
        if not question:
            question_id = kpi_line[Const.Q_ID]
            if question_id == "":
                Log.warning("The template has a survey question without ID or text")
                return False
            question = ('question_fk', int(question_id))
        answers = kpi_line[Const.ACCEPTED_ANSWER].split(',')
        min_answer = None if kpi_line[Const.REQUIRED_ANSWER] == '' else True
        for answer in answers:
            if self.survey.check_survey_answer(
                    survey_text=question, target_answer=answer, min_required_answer=min_answer):
                return True
        return False

    # availability:

    def calculate_availability_with_same_pack(self, relevant_template, relevant_scif, isnt_dp):
        """
        checks if all the lines in the availability sheet passes the KPI, AND if all of these filtered scif has
        at least one common product that has the same size and number of sub_packages.
        :param relevant_template: all the match lines from the availability sheet.
        :param relevant_scif: filtered scif
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we shouldn't calculate
        DP lines
        :return: boolean
        """
        packages = None
        for i, kpi_line in relevant_template.iterrows():
            if isnt_dp and kpi_line[Const.MANUFACTURER] in Const.DP_MANU:
                continue
            filtered_scif = self.filter_scif_availability(kpi_line, relevant_scif)
            filtered_scif = filtered_scif.fillna("NAN")
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
            if filtered_scif[filtered_scif['facings'] > 0]['facings'].count() < target:
                return False
        if len(packages) > 1:
            return False
        return True

    def calculate_availability(self, kpi_line, relevant_scif, isnt_dp):
        """
        checks if all the lines in the availability sheet passes the KPI (there is at least one product
        in this relevant scif that has the attributes).
        :param relevant_scif: filtered scif
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we shouldn't calculate
        DP lines
        :param kpi_line: line from the availability sheet
        :return: boolean
        """
        if isnt_dp and kpi_line[Const.MANUFACTURER] in Const.DP_MANU:
            return True
        filtered_scif = self.filter_scif_availability(kpi_line, relevant_scif)
        target = kpi_line[Const.TARGET]
        return filtered_scif[filtered_scif['facings'] > 0]['facings'].count() >= target

    def filter_scif_specific(self, relevant_scif, kpi_line, name_in_template, name_in_scif):
        """
        takes scif and filters it from the template
        :param relevant_scif: the current filtered scif
        :param kpi_line: line from one sheet (availability for example)
        :param name_in_template: the column name in the template
        :param name_in_scif: the column name in SCIF
        :return:
        """
        values = self.does_exist(kpi_line, name_in_template)
        if values:
            if name_in_scif in Const.NUMERIC_VALUES_TYPES:
                values = [float(x) for x in values]
            return relevant_scif[relevant_scif[name_in_scif].isin(values)]
        return relevant_scif

    def filter_scif_availability(self, kpi_line, relevant_scif):
        """
        calls filter_scif_specific for every column in the template of availability
        :param kpi_line:
        :param relevant_scif:
        :return:
        """
        names_of_columns = {
            Const.MANUFACTURER: "manufacturer_name",
            Const.BRAND: "brand_name",
            Const.TRADEMARK: "att2",
            Const.SIZE: "size",
            Const.NUM_SUB_PACKAGES: "number_of_sub_packages",
            # CCBOTTLERSUSConst.PREMIUM_SSD: "Premium SSD",
            # CCBOTTLERSUSConst.INNOVATION_BRAND: "Innovation Brand",
        }
        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(relevant_scif, kpi_line, name, names_of_columns[name])
        return relevant_scif

    # SOS:

    def calculate_sos(self, kpi_line, relevant_scif, isnt_dp, general_filters):
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1]
        # relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1]
        # num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        if isnt_dp:
            general_filters['manufacturer_name'] = (Const.DP_MANU, 0)
        target = self.get_sos_targets(self.store_id)
        general_filters[den_type] = den_value
        sos_filters = {num_type: num_value}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        if target:
            score = (sos_value / float(target))*100 if sos_value else 0
        else:
            score = sos_value
            target = 0
        return sos_value, score, target

    # SOS majority:

    def get_sos_targets(self, store):
        return False

    def calculate_sos_maj(self, kpi_line, relevant_scif, isnt_dp):
        """
        calculates SOS majority line in the relevant scif. Filters the denominator and sends the line to the
        match function (majority or dominant)
        :param kpi_line: line from SOS majority sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator (and the denominator of the dominant part).
        :return: boolean
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        if kpi_line[Const.EXCLUSION_SHEET] == Const.V:
            exclusion_sheet = self.templates[Const.SKU_EXCLUSION]
            relevant_exclusions = exclusion_sheet[exclusion_sheet[Const.KPI_NAME] == kpi_name]
            for i, exc_line in relevant_exclusions.iterrows():
                relevant_scif = self.exclude_scif(exc_line, relevant_scif)
        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
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
            Log.warning("SOS majority does not know '{}' part".format(kpi_line[Const.MAJ_DOM]))
            answer = False
        return answer

    def calculate_majority_part(self, kpi_line, relevant_scif, isnt_dp):
        """
        filters the numerator and checks if the SOS is bigger than 50%.
        :param kpi_line: line from SOS majority sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1]
        num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        num_type = kpi_line[Const.NUM_TYPES_2]
        num_value = kpi_line[Const.NUM_VALUES_2]
        num_scif = self.filter_by_type_value(num_scif, num_type, num_value)
        if num_scif.empty:
            return None
        if isnt_dp:
            num_scif = num_scif[~(num_scif['manufacturer_name'].isin(Const.DP_MANU))]
        target = Const.MAJORITY_TARGET
        return num_scif['facings'].sum() / relevant_scif['facings'].sum() >= target

    def calculate_dominant_part(self, kpi_line, relevant_scif, isnt_dp):
        """
        filters the numerator and checks if the given value in the given type is the one with the most facings.
        :param kpi_line: line from SOS majority sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out.
        :return: boolean
        """
        if isnt_dp:
            relevant_scif = relevant_scif[~(relevant_scif['manufacturer_name'].isin(Const.DP_MANU))]
        type_name = self.get_column_name(kpi_line[Const.NUM_TYPES_1], relevant_scif)
        values = str(kpi_line[Const.NUM_VALUES_1]).split(', ')
        if type_name in Const.NUMERIC_VALUES_TYPES:
            values = [float(x) for x in values]
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
        """
        checks what the real field name in DttFrame is (if it exists in the DF or exists in the "converter" sheet).
        :param field_name: str
        :param df: scif/products
        :return: real column name (if exists)
        """
        if field_name in df.columns:
            return field_name
        if field_name.upper() in self.converters[Const.NAME_IN_TEMP].str.upper().tolist():
            field_name = self.converters[self.converters[Const.NAME_IN_TEMP].str.upper() == field_name.upper()][
                Const.NAME_IN_DB].iloc[0]
            return field_name
        return None

    def filter_by_type_value(self, relevant_scif, type_name, value):
        """
        filters scif with the type and value
        :param relevant_scif: current filtered scif
        :param type_name: str (from the template)
        :param value: str
        :return: new scif
        """
        if type_name == "":
            return relevant_scif
        values = value.split(', ')
        new_type_name = self.get_column_name(type_name, relevant_scif)
        if not new_type_name:
            print "There is no field '{}'".format(type_name)
            return relevant_scif
        if new_type_name in Const.NUMERIC_VALUES_TYPES:
            values = [float(x) for x in values]
        return relevant_scif[relevant_scif[new_type_name].isin(values)]

    @staticmethod
    def exclude_scif(exclude_line, relevant_scif):
        """
        filters products out of the scif
        :param exclude_line: line from the exclusion sheet
        :param relevant_scif: current filtered scif
        :return: new scif
        """
        exclude_products = exclude_line[Const.PRODUCT_EAN].split(', ')
        return relevant_scif[~(relevant_scif['product_ean_code'].isin(exclude_products))]

    @staticmethod
    def does_exist(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does - returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return cell.split(", ")
        return None

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.SURVEY:
            return self.calculate_survey_specific
        elif kpi_type == Const.AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.SOS_MAJOR:
            return self.calculate_sos_maj
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def choose_and_write_results(self):
        """
        writes all the KPI in the DB: first the session's ones, second the scene's ones and in the end the ones
        that depends on the previous ones. After all it writes the red score
        """
        # self.scenes_results.to_csv('results/{}/scene {}.csv'.format(self.calculation_type, self.session_uid))####
        # self.session_results.to_csv('results/{}/session {}.csv'.format(self.calculation_type, self.session_uid))####
        main_template = self.templates[Const.KPIS]
        self.write_session_kpis(main_template)
        self.write_condition_kpis(main_template)
        self.write_missings(main_template)
        self.write_to_db(self.RED_SCORE, self.red_score)
        # result_dict = {Const.KPI_NAME: 'RED SCORE', Const.SCORE: self.red_score}####
        # self.all_results = self.all_results.append(result_dict, ignore_index=True)####
        # self.all_results.to_csv('results/{}/{}.csv'.format(self.calculation_type, self.session_uid))####

    def write_missings(self, main_template):
        """
        write 0 in all the KPIs that didn't get score
        :param main_template:
        """
        for i, main_line in main_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            if not self.all_results[self.all_results[Const.KPI_NAME] == kpi_name].empty:
                continue
            result = 0
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_session_kpis(self, main_template):
        """
        iterates all the session's KPIs and saves them
        :param main_template: main_sheet.
        """
        session_template = main_template[main_template[Const.CONDITION] == ""]
        if self.calculation_type == Const.SOVI:
            session_template = session_template[session_template[Const.SESSION_LEVEL] == Const.V]
        for i, main_line in session_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            result = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            if result.empty:
                continue
            result = result.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_incremental_kpis(self, scene_template):
        """
        lets the incremental KPIs choose their scenes (if they passed).
        if KPI passed some scenes, we will choose the scene that the children passed
        :param scene_template: filtered main_sheet
        :return: the new template (without the KPI written already)
        """
        incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        while not incremental_template.empty:
            for i, main_line in incremental_template.iterrows():
                kpi_name = main_line[Const.KPI_NAME]
                reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
                kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
                if not reuse_scene:
                    kpi_results = kpi_results[~(kpi_results[Const.SCENE_FK].isin(self.used_scenes))]
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
                    self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.RESULT], display_text,
                                             weight, scene_fk=scene_fk, reuse_scene=reuse_scene)
                    scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
            incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        return scene_template

    def write_regular_scene_kpis(self, scene_template):
        """
        lets the regular KPIs choose their scenes (if they passed).
        Like in the incremental part - if KPI passed some scenes, we will choose the scene that the children passed
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed incremental)
        :return: the new template (without the KPI written already)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.SCENE_FK].isin(self.used_scenes))]
            true_results = kpi_results[kpi_results[Const.RESULT] > 0]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if true_results.empty:
                continue
            true_results = true_results.sort_values(by=Const.RESULT, ascending=False)
            scene_fk = true_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.RESULT], display_text, weight,
                                     scene_fk=scene_fk, reuse_scene=reuse_scene)
            scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
        return scene_template

    def write_not_passed_scene_kpis(self, scene_template):
        """
        lets the KPIs not passed choose their scenes.
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed KPIs)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.SCENE_FK].isin(self.used_scenes))]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if kpi_results.empty:
                continue
            scene_fk = kpi_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, 0, display_text, weight, scene_fk=scene_fk, reuse_scene=reuse_scene)

    def write_scene_kpis(self, main_template):
        """
        iterates every scene_kpi that does not depend on others, and choose the scene they will take:
        1. the incrementals take their scene (if they passed).
        2. the regular KPIs that passed choose their scenes.
        3. the ones that didn't pass choose their random scenes.
        :param main_template: main_sheet.
        """
        scene_template = main_template[(main_template[Const.SESSION_LEVEL] != Const.V) &
                                       (main_template[Const.CONDITION] == "")]
        scene_template = self.write_incremental_kpis(scene_template)
        scene_template = self.write_regular_scene_kpis(scene_template)
        self.write_not_passed_scene_kpis(scene_template)

    def write_condition_kpis(self, main_template):
        """
        writes all the KPI that depend on other KPIs by checking if the parent KPI has passed and in which scene.
        :param main_template: main_sheet
        """
        condition_template = main_template[main_template[Const.CONDITION] != '']
        for i, main_line in condition_template.iterrows():
            condition = main_line[Const.CONDITION]
            kpi_name = main_line[Const.KPI_NAME]
            if self.calculation_type == Const.MANUAL or main_line[Const.SESSION_LEVEL] == Const.V:
                kpi_results = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            else:
                kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            condition_result = self.all_results[(self.all_results[Const.KPI_NAME] == condition) &
                                                (self.all_results[Const.RESULT] > 0)]
            if condition_result.empty:
                continue
            condition_result = condition_result.iloc[0]
            condition_scene = condition_result[Const.SCENE_FK]
            if condition_scene and Const.SCENE_FK in kpi_results:
                results = kpi_results[kpi_results[Const.SCENE_FK] == condition_scene]
            else:
                results = kpi_results
            if results.empty:
                continue
            result = results.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            scene_fk = results.iloc[0][Const.SCENE_FK] if Const.SCENE_FK in kpi_results else None
            self.write_to_all_levels(kpi_name, result, display_text, weight, scene_fk=scene_fk)

    def get_united_scenes(self):
        return self.scif[self.scif['United Deliver'] == 'Y']['scene_id'].unique().tolist()

    def get_weight_factor(self):
        sum_weights = self.templates[Const.KPIS][Const.WEIGHT].sum()
        return sum_weights / 100.0

    def get_score(self, weight):
        return weight / self.weight_factor

    def write_to_db(self, kpi_name, score, result=None, threshold=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        self.write_to_db_result(
            self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 2), score=score, level=2)
        self.write_to_db_result(
            self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 3), score=score, level=3, display_text=kpi_name,
            threshold=threshold, result=result)

    def write_to_db_result(self, fk, level, score, set_type=Const.SOVI, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        if kwargs:
            kwargs['score'] = score
            attributes = self.create_attributes_dict(fk=fk, level=level, set_type=set_type, **kwargs)
        else:
            attributes = self.create_attributes_dict(fk=fk, score=score, set_type=set_type, level=level)
        if level == self.common_db.LEVEL1:
            table = self.common_db.KPS_RESULT
        elif level == self.common_db.LEVEL2:
            table = self.common_db.KPK_RESULT
        elif level == self.common_db.LEVEL3:
            table = self.common_db.KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        if set_type == Const.SOVI:
            self.common_db.kpi_results_queries.append(query)
        else:
            self.common_db_integ.kpi_results_queries.append(query)

    def create_attributes_dict(self, score, fk=None, level=None, display_text=None, set_type=Const.SOVI, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.
        or
        you can send dict with all values in kwargs
        """
        kpi_static_data = self.kpi_static_data if set_type == Const.SOVI else self.kpi_static_data_integ
        if level == self.common_db.LEVEL1:
            if kwargs:
                kwargs['score'] = score
                values = [val for val in kwargs.values()]
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame(values, columns=col)
            else:
                kpi_set_name = kpi_static_data[kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
                attributes = pd.DataFrame(
                    [(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                      format(score, '.2f'), fk)],
                    columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1', 'kpi_set_fk'])
        elif level == self.common_db.LEVEL2:
            if kwargs:
                kwargs['score'] = score
                values = [val for val in kwargs.values()]
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame(values, columns=col)
            else:
                kpi_name = kpi_static_data[kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
                attributes = pd.DataFrame(
                    [(self.session_uid, self.store_id, self.visit_date.isoformat(), fk, kpi_name, score)],
                    columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.common_db.LEVEL3:
            if kwargs:
                kwargs['score'] = score
                values = tuple([val for val in kwargs.values()])
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame([values], columns=col)
            else:
                data = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]
                kpi_fk = data['kpi_fk'].values[0]
                kpi_set_name = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
                attributes = pd.DataFrame(
                    [(display_text, self.session_uid, kpi_set_name, self.store_id, self.visit_date.isoformat(),
                      datetime.utcnow().isoformat(), score, kpi_fk, fk)],
                    columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                             'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def commit_results(self):
        """
        committing the results in both sets
        """
        self.common_db.delete_results_data_by_kpi_set()
        self.common_db.commit_results_data_without_delete()
        if self.common_db_integ:
            self.common_db_integ.delete_results_data_by_kpi_set()
            self.common_db_integ.commit_results_data_without_delete()
