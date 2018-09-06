from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

__author__ = 'Elyashiv'


class FunctionsToolBox:

    def __init__(self, data_provider, output, templates, store_attr):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.survey = Survey(self.data_provider, self.output)
        self.templates = templates
        self.converters = self.templates[Const.CONVERTERS]
        self.exclusion_sheet = self.templates[Const.SKU_EXCLUSION]
        self.store_attr = store_attr

    # survey:

    def calculate_survey_specific(self, kpi_line, relevant_scif=None, isnt_dp=None):
        """
        returns a survey line if True or False
        :param kpi_line: line from the survey sheet
        :param relevant_scif: unused, for the general function
        :param isnt_dp: unused, for the general function
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

    # pointer the other kpi_functions:

    def calculate_kpi_by_type(self, main_line, filtered_scif):
        """
        the function calculates all the kpis
        :param main_line: one kpi line from the main template
        :param filtered_scif:
        :return: boolean, but it can be None if we want not to write it in DB
        """
        kpi_type = main_line[Const.SHEET]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == main_line[Const.KPI_NAME]]
        target = len(relevant_template) if main_line[Const.GROUP_TARGET] == Const.ALL else main_line[Const.GROUP_TARGET]
        isnt_dp = True if self.store_attr != Const.DP and main_line[Const.STORE_ATTRIBUTE] == Const.DP else False
        if main_line[Const.SAME_PACK] == Const.V:
            sizes = filtered_scif['size'].tolist()
            sub_packages_nums = filtered_scif['number_of_sub_packages'].tolist()
            packages = set(zip(sizes, sub_packages_nums))
            for package in packages:
                filtered_scif = filtered_scif[(filtered_scif['size'] == package[0]) &
                                              (filtered_scif['number_of_sub_packages'] == package[1])]
                result = self.calculate_specific_kpi(relevant_template, filtered_scif, isnt_dp,
                                                     target, self.calculate_availability)
                if result is False:
                    return result
            return True
        kpi_function = self.get_kpi_function(kpi_type)
        return self.calculate_specific_kpi(relevant_template, filtered_scif, isnt_dp, target, kpi_function)

    @staticmethod
    def calculate_specific_kpi(relevant_template, filtered_scif, isnt_dp, target, kpi_function):
        """
        checks if the passed lines are more than target
        :param relevant_template: specific template filtered with the specific kpi lines
        :param filtered_scif:
        :param isnt_dp: the main_line has "DP" flag and the store_attr is not DP
        :param target: integer
        :param kpi_function: specific function for the calculation
        :return: boolean, but it can be None if we want not to write it in DB
        """
        passed_counter = 0
        for i, kpi_line in relevant_template.iterrows():
            answer = kpi_function(kpi_line, filtered_scif, isnt_dp)
            if answer:
                passed_counter += 1
            elif answer is None:
                return None
        return passed_counter >= target

    # availability:

    def calculate_availability(self, kpi_line, relevant_scif, isnt_dp):  # V
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

    def filter_scif_availability(self, kpi_line, relevant_scif):  # V
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
            # CCBOTTLERSUSCCBOTTLERSUS_SANDConst.PREMIUM_SSD: "Premium SSD",
            # CCBOTTLERSUSCCBOTTLERSUS_SANDConst.INNOVATION_BRAND: "Innovation Brand",
        }
        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(relevant_scif, kpi_line, name, names_of_columns[name])
        return relevant_scif

    def filter_scif_specific(self, relevant_scif, kpi_line, name_in_template, name_in_scif):  # V
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

    # SOS:

    def sos_first_filtering(self, kpi_line, relevant_scif):
        """
        The common part of both SOS functions, first filtering
        :param kpi_line: line from the SOS/SOS_maj template
        :param relevant_scif: filtered scif
        :return: new filtered scif
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        if kpi_line[Const.EXCLUSION_SHEET] == Const.V:
            relevant_exclusions = self.exclusion_sheet[self.exclusion_sheet[Const.KPI_NAME] == kpi_name]
            for i, exc_line in relevant_exclusions.iterrows():
                relevant_scif = self.exclude_scif(exc_line, relevant_scif)
        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1]
        relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        return relevant_scif

    def calculate_sos(self, kpi_line, relevant_scif, isnt_dp):  # EXCLUSION
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        relevant_scif = self.sos_first_filtering(kpi_line, relevant_scif)
        if kpi_line[Const.SSD_STILL] != "":
            relevant_scif = self.filter_by_type_value(
                relevant_scif, Const.SSD_STILL, kpi_line[Const.SSD_STILL])
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1]
        num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        if isnt_dp:
            num_scif = num_scif[~(num_scif['manufacturer_name'].isin(Const.DP_MANU))]
        target = float(kpi_line[Const.TARGET]) / 100
        percentage = num_scif['facings'].sum() / relevant_scif['facings'].sum() if relevant_scif['facings'].sum() > 0 \
            else 0
        return percentage >= target

    # SOS majority:

    def calculate_sos_maj(self, kpi_line, relevant_scif, isnt_dp):  # EXCLUSION
        """
        calculates SOS majority line in the relevant scif. Filters the denominator and sends the line to the
        match function (majority or dominant)
        :param kpi_line: line from SOS majority sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator (and the denominator of the dominant part).
        :return: boolean
        """
        relevant_scif = self.sos_first_filtering(kpi_line, relevant_scif)
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

    def calculate_majority_part(self, kpi_line, relevant_scif, isnt_dp):  # V
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

    def calculate_dominant_part(self, kpi_line, relevant_scif, isnt_dp):  # V
        """
        filters the numerator and checks if the given value in the given type is the one with the most facings.
        :param kpi_line: line from SOS majority sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out.
        :return: boolean
        """
        type_name = self.get_column_name(kpi_line[Const.NUM_TYPES_1], relevant_scif)
        values = str(kpi_line[Const.NUM_VALUES_1]).split(', ')
        if isnt_dp:
            relevant_scif = relevant_scif[~(relevant_scif['manufacturer_name'].isin(Const.DP_MANU))]
            if kpi_line[Const.ADD_IF_NOT_DP] != "":
                values_to_add = str(kpi_line[Const.ADD_IF_NOT_DP]).split(', ')
                values = values + values_to_add
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
