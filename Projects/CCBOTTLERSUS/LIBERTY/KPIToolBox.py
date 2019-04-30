import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.LIBERTY.Data.Const import Const
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Hunter'


class LIBERTYToolBox:

    def __init__(self, data_provider, output, common_db):
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
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_info = self.ps_data_provider.get_ps_store_info(self.data_provider[Data.STORE_INFO])
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[self.scif['product_type'] != "Irrelevant"]
        self.templates = {}
        self.result_values = self.ps_data_provider.get_result_values()
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.common_db = common_db
        self.survey = Survey(self.data_provider, output=self.output, ps_data_provider=self.ps_data_provider,
                             common=self.common_db)
        self.manufacturer_fk = Const.MANUFACTURER_FK
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.retailer = self.store_info['retailer_name'].iloc[0]
        self.branch = self.store_info['branch_name'].iloc[0]
        self.additional_attribute_4 = self.store_info['additional_attribute_4'].iloc[0]
        self.additional_attribute_7 = self.store_info['additional_attribute_7'].iloc[0]
        self.body_armor_delivered = self.get_body_armor_delivery_status()

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        if self.region != 'Liberty':
            return
        red_score = 0
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            relevant_store_types = self.does_exist(main_line, Const.ADDITIONAL_ATTRIBUTE_7)
            if relevant_store_types and self.additional_attribute_7 not in relevant_store_types:
                continue
            result = self.calculate_main_kpi(main_line)
            if result:
                red_score += main_line[Const.WEIGHT]

        if len(self.common_db.kpi_results) > 0:
            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(Const.RED_SCORE_PARENT)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=1, denominator_id=self.store_id, result=red_score,
                                              identifier_result=Const.RED_SCORE_PARENT, should_enter=True)
        return

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        relevant_scif = self.scif
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        excluded_scene_types = self.does_exist(main_line, Const.EXCLUDED_SCENE_TYPE)
        if excluded_scene_types:
            relevant_scif = relevant_scif[~relevant_scif['template_name'].isin(excluded_scene_types)]
        template_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
        if template_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(template_groups)]

        result = self.calculate_kpi_by_type(main_line, relevant_scif)

        return result

    def calculate_kpi_by_type(self, main_line, relevant_scif):
        """
        the function calculates all the kpis
        :param main_line: one kpi line from the main template
        :param relevant_scif:
        :return: boolean, but it can be None if we want not to write it in DB
        """
        kpi_type = main_line[Const.KPI_TYPE]
        relevant_template = self.templates[kpi_type]
        kpi_line = relevant_template[relevant_template[Const.KPI_NAME] == main_line[Const.KPI_NAME]].iloc[0]
        kpi_function = self.get_kpi_function(kpi_type)
        weight = main_line[Const.WEIGHT]

        if relevant_scif.empty:
            result = 0
        else:
            result = kpi_function(kpi_line, relevant_scif, weight)

        result_type_fk = self.ps_data_provider.get_pks_of_result(
            Const.PASS) if result > 0 else self.ps_data_provider.get_pks_of_result(Const.FAIL)

        kpi_name = kpi_line[Const.KPI_NAME] + Const.LIBERTY
        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_name)
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                          denominator_id=self.store_id, denominator_result=0, weight=weight,
                                          result=result_type_fk, identifier_parent=Const.RED_SCORE_PARENT,
                                          identifier_result=kpi_name, should_enter=True)

        return result

    # SOS functions
    def calculate_sos(self, kpi_line, relevant_scif, weight):
        market_share_required = self.does_exist(kpi_line, Const.MARKET_SHARE_TARGET)
        if market_share_required:
            market_share_target = self.get_market_share_target()
        else:
            market_share_target = 0

        if not market_share_target:
            market_share_target = 0

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            relevant_scif = relevant_scif[relevant_scif['manufacturer_name'].isin(manufacturer)]

        number_of_facings = relevant_scif['facings'].sum()
        result = 1 if number_of_facings > market_share_target else 0

        parent_kpi_name = kpi_line[Const.KPI_NAME] + Const.LIBERTY
        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(parent_kpi_name + Const.DRILLDOWN)
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                          denominator_id=self.store_id, denominator_result=0, weight=weight,
                                          result=number_of_facings, target=market_share_target,
                                          identifier_parent=parent_kpi_name, should_enter=True)

        return result

    # Availability functions
    def calculate_availability(self, kpi_line, relevant_scif, weight):
        survey_question_skus_required = self.does_exist(kpi_line, Const.SURVEY_QUESTION_SKUS_REQUIRED)
        if survey_question_skus_required:
            survey_question_skus = self.get_relevant_product_assortment_by_kpi_name(kpi_line[Const.KPI_NAME])
            unique_skus = \
                relevant_scif[relevant_scif['product_fk'].isin(survey_question_skus)]['product_fk'].unique().tolist()
        else:
            manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
            if manufacturer:
                relevant_scif = relevant_scif[relevant_scif['manufacturer_name'].isin(manufacturer)]
            brand = self.does_exist(kpi_line, Const.BRAND)
            if brand:
                relevant_scif = relevant_scif[relevant_scif['brand_name'].isin(brand)]
            category = self.does_exist(kpi_line, Const.CATEGORY)
            if category:
                relevant_scif = relevant_scif[relevant_scif['category'].isin(category)]
            excluded_brand = self.does_exist(kpi_line, Const.EXCLUDED_BRAND)
            if excluded_brand:
                relevant_scif = relevant_scif[~relevant_scif['brand_name'].isin(excluded_brand)]
            unique_skus = relevant_scif['product_fk'].unique().tolist()

        length_of_unique_skus = len(unique_skus)
        minimum_number_of_skus = kpi_line[Const.MINIMUM_NUMBER_OF_SKUS]

        result = 1 if length_of_unique_skus >= minimum_number_of_skus else 0

        parent_kpi_name = kpi_line[Const.KPI_NAME] + Const.LIBERTY
        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(parent_kpi_name + Const.DRILLDOWN)
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                          denominator_id=self.store_id, denominator_result=0, weight=weight,
                                          result=length_of_unique_skus, target=minimum_number_of_skus,
                                          identifier_parent=parent_kpi_name, should_enter=True)

        return result

    def get_relevant_product_assortment_by_kpi_name(self, kpi_name):
        template = self.templates[Const.SURVEY_QUESTION_SKUS]
        relevant_template = template[template[Const.KPI_NAME] == kpi_name]
        relevant_ean_codes = relevant_template[Const.EAN_CODE].unique().tolist()
        relevant_ean_codes = [str(int(x)) for x in relevant_ean_codes if x != '']  # we need this to fix dumb template
        relevant_products = self.all_products[self.all_products['product_ean_code'].isin(relevant_ean_codes)]
        return relevant_products['product_fk'].unique().tolist()

    # Count of Display functions
    def calculate_count_of_display(self, kpi_line, relevant_scif, weight):
        filtered_scif = relevant_scif

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            filtered_scif = relevant_scif[relevant_scif['manufacturer_name'].isin(manufacturer)]

        brand = self.does_exist(kpi_line, Const.BRAND)
        if brand:
            filtered_scif = filtered_scif[filtered_scif['brand_name'].isin(brand)]

        ssd_still = self.does_exist(kpi_line, Const.ATT4)
        if ssd_still:
            filtered_scif = filtered_scif[filtered_scif['att4'].isin(ssd_still)]

        size_subpackages = self.does_exist(kpi_line, Const.SIZE_SUBPACKAGES_NUM)
        if size_subpackages:
            # convert all pairings of size and number of subpackages to tuples
            size_subpackages_tuples = [tuple([float(i) for i in x.split(';')]) for x in size_subpackages]
            filtered_scif = filtered_scif[pd.Series(list(zip(filtered_scif['size'],
                                                             filtered_scif['number_of_sub_packages'])),
                                                    index=filtered_scif.index).isin(size_subpackages_tuples)]

        sub_packages = self.does_exist(kpi_line, Const.SUBPACKAGES_NUM)
        if sub_packages:
            if sub_packages == [Const.NOT_NULL]:
                filtered_scif = filtered_scif[~filtered_scif['number_of_sub_packages'].isnull()]
            else:
                filtered_scif = filtered_scif[filtered_scif['number_of_sub_packages'].isin([int(i) for i in sub_packages])]

        if self.does_exist(kpi_line, Const.MINIMUM_FACINGS_REQUIRED):
            number_of_passing_displays = self.get_number_of_passing_displays(filtered_scif)

            parent_kpi_name = kpi_line[Const.KPI_NAME] + Const.LIBERTY
            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(parent_kpi_name + Const.DRILLDOWN)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                              denominator_id=self.store_id, denominator_result=0, weight=weight,
                                              result=number_of_passing_displays,
                                              identifier_parent=parent_kpi_name, should_enter=True)
            return 1 if number_of_passing_displays > 0 else 0
        else:
            return 0

    # Share of Display functions
    def calculate_share_of_display(self, kpi_line, relevant_scif, weight):
        filtered_scif = relevant_scif

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            filtered_scif = relevant_scif[relevant_scif['manufacturer_name'].isin(manufacturer)]

        ssd_still = self.does_exist(kpi_line, Const.ATT4)
        if ssd_still:
            filtered_scif = filtered_scif[filtered_scif['att4'].isin(ssd_still)]

        if self.does_exist(kpi_line, Const.MARKET_SHARE_TARGET):
            market_share_target = self.get_market_share_target(ssd_still=ssd_still)
        else:
            market_share_target = 0

        if self.does_exist(kpi_line, Const.INCLUDE_BODY_ARMOR) and self.body_armor_delivered:
            body_armor_scif = relevant_scif[relevant_scif['brand_fk'] == Const.BODY_ARMOR_BRAND_FK]
            filtered_scif = filtered_scif.append(body_armor_scif, sort=False)

        if self.does_exist(kpi_line, Const.MINIMUM_FACINGS_REQUIRED):
            number_of_passing_displays = self.get_number_of_passing_displays(filtered_scif)

            result = 1 if number_of_passing_displays > market_share_target else 0

            parent_kpi_name = kpi_line[Const.KPI_NAME] + Const.LIBERTY
            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(parent_kpi_name + Const.DRILLDOWN)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                              denominator_id=self.store_id, denominator_result=0, weight=weight,
                                              result=number_of_passing_displays, target=market_share_target,
                                              identifier_parent=parent_kpi_name, should_enter=True)

            return result
        else:
            return 0

    def get_number_of_passing_displays(self, filtered_scif):
        if filtered_scif.empty:
            return 0

        filtered_scif['passed_displays'] = \
            filtered_scif.apply(lambda row: self._calculate_pass_status_of_display(row), axis=1)

        return filtered_scif['passed_displays'].sum()

    def _calculate_pass_status_of_display(self, row):  # need to move to external KPI targets
        template = self.templates[Const.MINIMUM_FACINGS]
        package_category = (row['size'], row['number_of_sub_packages'], row['size_unit'])
        relevant_template = template[pd.Series(zip(template['size'],
                                                   template['subpackages_num'],
                                                   template['unit_of_measure'])) == package_category]
        minimum_facings = relevant_template[Const.MINIMUM_FACINGS_REQUIRED_FOR_DISPLAY].min()
        return 1 if row['facings'] > minimum_facings else 0

    # Survey functions
    def calculate_survey(self, kpi_line, relevant_scif, weight):
        return 1 if self.survey.check_survey_answer(kpi_line[Const.QUESTION_TEXT], 'Yes') else 0

    # helper functions
    def get_market_share_target(self, ssd_still=None):  # need to move to external KPI targets
        template = self.templates[Const.MARKET_SHARE]
        relevant_template = template[(template[Const.ADDITIONAL_ATTRIBUTE_4] == self.additional_attribute_4) &
                                     (template[Const.RETAILER] == self.retailer) &
                                     (template[Const.BRANCH] == self.branch)]

        if relevant_template.empty:
            if ssd_still:
                if ssd_still[0].lower() == Const.SSD.lower():
                    return 49
                elif ssd_still[0].lower() == Const.STILL.lower():
                    return 16
                else:
                    return 0
            else:
                return 26

        if ssd_still:
            if ssd_still[0].lower() == Const.SSD.lower():
                return relevant_template[Const.SSD].iloc[0]
            elif ssd_still[0].lower() == Const.STILL.lower():
                return relevant_template[Const.STILL].iloc[0]

        # total 26, ssd only 49, still only 16
        return relevant_template[Const.SSD_AND_STILL].iloc[0]

    def get_body_armor_delivery_status(self):
        if self.store_info['additional_attribute_8'].iloc[0] == 'Y':
            return True
        else:
            return False

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == Const.COUNT_OF_DISPLAY:
            return self.calculate_count_of_display
        elif kpi_type == Const.SHARE_OF_DISPLAY:
            return self.calculate_share_of_display
        elif kpi_type == Const.SURVEY:
            return self.calculate_survey
        else:
            Log.warning(
                "The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

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
                return [x.strip() for x in cell.split(",")]
        return None
