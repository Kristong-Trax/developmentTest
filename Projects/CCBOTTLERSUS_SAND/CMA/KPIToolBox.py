import os
from datetime import datetime
import pandas as pd
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import Shared
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.CMA.Const import Const
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


__author__ = 'Uri'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'CMA Compliance Template v0.9.xlsx')

STORE_TYPES = {
    "CR SOVI RED": "CR&LT",
    "DRUG SOVI RED": "Drug",
    "VALUE SOVI RED": "Value",
    "FSOP - QSR": "QSR",
}
SUB_PROJECT = 'CMA Compliance'


class CMAToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, common_db2):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.manufacturer_fk = 1
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.united_scenes = self.get_united_scenes()  # we don't need to check scenes without United products
        self.survey = Survey(self.data_provider, self.output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.sos = SOS(self.data_provider, self.output)
        self.templates = {}
        self.common_db = Common(self.data_provider, SUB_PROJECT)
        self.common_db2 = common_db2
        self.result_values = self.ps_data_provider.get_result_values()
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_14'].iloc[0]
        self.sales_center = self.store_info['additional_attribute_5'].iloc[0]
        if self.store_type in STORE_TYPES:
            self.store_type = STORE_TYPES[self.store_type]
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.kpi_static_data = self.common_db.get_kpi_static_data()
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.total_score = 0
        self.total_count = 0
        for sheet in Const.SHEETS_CMA:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.tools = Shared(self.data_provider, self.output)

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        if self.region in Const.REGIONS:
            for i, main_line in main_template.iterrows():
                store_type = self.does_exist(main_line, Const.STORE_TYPE)
                if store_type is None or self.store_type in self.does_exist(main_line, Const.STORE_TYPE):
                    self.calculate_main_kpi(main_line)
            kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(SUB_PROJECT)

            result = 0
            if self.total_count:
                result = self.total_score * 100.0 / self.total_count
            self.common_db2.write_to_db_result(fk=kpi_fk, result=result, numerator_result=self.total_score,
                                               numerator_id=self.manufacturer_fk, denominator_result=self.total_count,
                                               denominator_id=self.store_id,
                                               identifier_result=self.common_db2.get_dictionary(parent_name=SUB_PROJECT))
            self.write_to_db_result(
                self.common_db.get_kpi_fk_by_kpi_name(SUB_PROJECT, 1), score=self.total_score, level=1)

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        relevant_scif = self.scif[self.scif['scene_id'].isin(self.united_scenes)]
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        result = score = target = None
        general_filters = {}
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        scene_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            general_filters['template_group'] = scene_groups
        if kpi_type == Const.SOS:
            isnt_dp = True if self.store_attr != Const.DP and main_line[Const.STORE_ATTRIBUTE] == Const.DP else False
            relevant_template = self.templates[kpi_type]
            relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
            kpi_function = self.get_kpi_function(kpi_type)
            for i, kpi_line in relevant_template.iterrows():
                result, score, target = kpi_function(kpi_line, relevant_scif, isnt_dp, general_filters)
        else:
            pass
        self.total_count += 1
        if score > 0:
            self.total_score += 1
        if isinstance(result, tuple):
            self.write_to_all_levels(kpi_name=kpi_name, result=result[0], score=score, target=target,
                                     num=result[1], den=result[2])
        else:
            self.write_to_all_levels(kpi_name=kpi_name, result=result, score=score, target=target)

    # write in DF:
    def write_to_all_levels(self, kpi_name, result, score, target=None, num=None, den=None):
        """
        Writes the final result in the "all" DF, add the score to the red score and writes the KPI in the DB
        :param kpi_name: str
        :param result: int
        :param display_text: str
        :param weight: int/float
        :param scene_fk: for the scene's kpi
        :param reuse_scene: this kpi can use scenes that were used
        """
        # result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result, Const.SCORE: score, Const.THRESHOLD: target}
        # self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.write_to_db(kpi_name, score, result=result, target=target, num=num, den=den)

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
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1].split(',')
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1].split(',')
        target = self.get_sos_targets(kpi_name)
        general_filters[den_type] = den_value
        if kpi_line[Const.DEN_TYPES_2]:
            den_type_2 = kpi_line[Const.DEN_TYPES_2]
            den_value_2 = kpi_line[Const.DEN_VALUES_2].split(',')
            general_filters[den_type_2] = den_value_2
        sos_filters = {num_type: num_value}
        if isnt_dp:
            sos_filters['manufacturer_name'] = (Const.DP_MANU, 0)
        if kpi_line[Const.NUM_TYPES_2]:
            num_type_2 = kpi_line[Const.NUM_TYPES_2]
            num_value_2 = kpi_line[Const.NUM_VALUES_2].split(',')
            sos_filters[num_type_2] = num_value_2

        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **sos_filters)]
        den_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        sos_value, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)
        # sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        # sos_value *= 100
        # sos_value = round(sos_value, 2)

        if target:
            target *= 100
            score = 1 if sos_value >= target else 0
        else:
            score = 0
            target = 0
        return (sos_value, num, den), score, target

    # SOS majority:

    def get_sos_targets(self, kpi_name):
        targets_template = self.templates[Const.TARGETS]
        store_targets = targets_template.loc[(targets_template['program'] == self.program) &
                                             (targets_template['channel'] == self.store_type)]
        filtered_targets_to_kpi = store_targets.loc[targets_template['KPI name'] == kpi_name]
        if not filtered_targets_to_kpi.empty:
            target = filtered_targets_to_kpi[Const.TARGET].values[0]
        else:
            target = None
        return target
        # return False

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

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUPNGAMERICAGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition


    # helpers:
    @staticmethod
    def get_column_name(field_name, df):
        """
        checks what the real field name in DttFrame is (if it exists in the DF or exists in the "converter" sheet).
        :param field_name: str
        :param df: scif/products
        :return: real column name (if exists)
        """
        if field_name in df.columns:
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
                if ", " in cell:
                    return cell.split(", ")
                else:
                    return cell.split(',')
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

    def get_united_scenes(self):
        return self.scif[self.scif['United Deliver'] == 'Y']['scene_id'].unique().tolist()

    def get_pks_of_result(self, result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    def write_to_db(self, kpi_name, score, result=None, target=None, num=None, den=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param result: str
        :param target: int
        """
        if target and score == 0:
            delta = den * (target / 100) - num
        else:
            delta = 0
        score_value = Const.PASS if score == 1 else Const.FAIL
        score = self.get_pks_of_result(score_value)
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(SUB_PROJECT + " " + kpi_name)
        self.common_db2.write_to_db_result(fk=kpi_fk, result=result, score=score, should_enter=True, target=target,
                                           numerator_result=num, denominator_result=den, weight=delta,
                                           numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id,
                                           identifier_parent=self.common_db2.get_dictionary(parent_name=SUB_PROJECT))
        self.write_to_db_result(
            self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 2), score=score, level=2)
        self.write_to_db_result(
            self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 3), score=score, level=3,
            threshold=target, result=result)

    def write_to_db_result(self, fk, level, score, set_type=Const.SOVI, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        if kwargs:
            kwargs['score'] = score
            attributes = self.create_attributes_dict(fk=fk, level=level, **kwargs)
        else:
            attributes = self.create_attributes_dict(fk=fk, score=score, level=level)
        if level == self.common_db.LEVEL1:
            table = self.common_db.KPS_RESULT
        elif level == self.common_db.LEVEL2:
            table = self.common_db.KPK_RESULT
        elif level == self.common_db.LEVEL3:
            table = self.common_db.KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.common_db.kpi_results_queries.append(query)

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
            data = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            display_text = data['kpi_name'].values[0]
            if kwargs:
                kwargs = self.add_additional_data_to_attributes(kwargs, score, kpi_set_name, kpi_fk, fk,
                                                                datetime.utcnow().isoformat(), display_text)

                values = tuple([val for val in kwargs.values()])
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame([values], columns=col)
            else:
                attributes = pd.DataFrame(
                    [(display_text, self.session_uid, kpi_set_name, self.store_id, self.visit_date.isoformat(),
                      datetime.utcnow().isoformat(), score, kpi_fk, fk)],
                    columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                             'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def add_additional_data_to_attributes(self, kwargs_dict, score, kpi_set_name, kpi_fk, fk, calc_time, display_text):
        kwargs_dict['score'] = score
        kwargs_dict['kps_name'] = kpi_set_name
        kwargs_dict['kpi_fk'] = kpi_fk
        kwargs_dict['atomic_kpi_fk'] = fk
        kwargs_dict['calculation_time'] = calc_time
        kwargs_dict['session_uid'] = self.session_uid
        kwargs_dict['store_fk'] = self.store_id
        kwargs_dict['visit_date'] = self.visit_date.isoformat()
        kwargs_dict['display_text'] = display_text
        return kwargs_dict

    def commit_results(self):
        """
        committing the results in both sets
        """
        self.common_db.delete_results_data_by_kpi_set()
        self.common_db.commit_results_data_without_delete()

