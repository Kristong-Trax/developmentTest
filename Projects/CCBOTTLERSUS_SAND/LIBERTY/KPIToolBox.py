from datetime import datetime
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.LIBERTY.Data.Const import Const
from KPIUtils_v2.DB.Common import Common
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[self.scif['product_type'] != "Irrelevant"]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.templates = {}
        self.result_values = self.ps_data_provider.get_result_values()
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.common_db = common_db
        self.manufacturer_fk = Const.MANUFACTURER_FK
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.retailer = self.store_info['retailer_name'].iloc[0]
        # self.branch = self.store_info['branch_name'].iloc[0]
        # self.body_armor_delivered = self.get_body_armor_delivery_status()

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        red_score = 0
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            relevant_store_types = self.does_exist(main_line, Const.STORE_TYPE)
            if relevant_store_types and self.store_type not in relevant_store_types:
                continue
            result = self.calculate_main_kpi(main_line)
            if result:
                red_score += main_line[Const.WEIGHT]

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

    def calculate_kpi_by_type(self, main_line, filtered_scif):
        """
        the function calculates all the kpis
        :param main_line: one kpi line from the main template
        :param filtered_scif:
        :return: boolean, but it can be None if we want not to write it in DB
        """
        kpi_type = main_line[Const.KPI_TYPE]
        relevant_template = self.templates[kpi_type]
        kpi_line = relevant_template[relevant_template[Const.KPI_NAME] == main_line[Const.KPI_NAME]]
        kpi_function = self.get_kpi_function(kpi_type)

        return kpi_function(kpi_line, filtered_scif)

    # SOS functions
    def calculate_sos(self, kpi_line, relevant_scif):
        return
        market_share_required = self.does_exist(kpi_line, Const.MARKET_SHARE_TARGET)
        if market_share_required:
            market_share_target = self.get_market_share_target()
        else:
            market_share_target = 0

        if not market_share_target:
            market_share_target = 0

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            relevant_scif = relevant_scif[relevant_scif['manufacturer_name'] == manufacturer]

        result = relevant_scif['facings'].sum() > market_share_target

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME] + Const.LIBERTY)
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                          denominator_id=self.store_id, denominator_result=0,
                                          result=result, identifier_parent=Const.RED_SCORE_PARENT, should_enter=True)

        return result

    # Availability functions
    def calculate_availability(self, kpi_line, relevant_scif):
        survey_question_skus_required = self.does_exist(kpi_line, Const.SURVEY_QUESTION_SKUS_REQUIRED)
        if survey_question_skus_required:
            survey_question_skus = self.get_kpi_function(kpi_line[Const.KPI_NAME])
            unique_skus = \
                relevant_scif[relevant_scif['product_fk'].isin(survey_question_skus)]['product_fk'].unique().tolist()
        else:
            manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
            if manufacturer:
                relevant_scif = relevant_scif[relevant_scif['manufacturer_name'] == manufacturer]
            brand = self.does_exist(kpi_line, Const.BRAND)
            if brand:
                relevant_scif = relevant_scif[relevant_scif['brand_name'] == brand]
            category = self.does_exist(kpi_line, Const.CATEGORY)
            if category:
                relevant_scif = relevant_scif[relevant_scif['category'] == category]
            excluded_brand = self.does_exist(kpi_line, Const.EXCLUDED_BRAND)
            if excluded_brand:
                relevant_scif = relevant_scif[~relevant_scif['brand_name'] == excluded_brand]
            unique_skus = relevant_scif['product_fk'].unique().tolist()

        result = len(unique_skus) >= kpi_line[Const.MINIMUM_NUMBER_OF_SKUS]

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME] + Const.LIBERTY)
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                          denominator_id=self.store_id, denominator_result=0,
                                          result=result, identifier_parent=Const.RED_SCORE_PARENT, should_enter=True)

        return result

    def get_relevant_product_assortment_by_kpi_name(self, kpi_name):
        template = self.templates[Const.SURVEY_QUESTION_SKUS]
        relevant_template = template[template[Const.KPI_NAME] == kpi_name]
        return relevant_template['product_fk'].unique().tolist()

    # Count of Display functions
    def calculate_count_of_display(self, kpi_line, relevant_scif):
        filtered_scif = relevant_scif

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            filtered_scif = relevant_scif[relevant_scif['manufacturer_name'] == manufacturer]

        brand = self.does_exist(kpi_line, Const.BRAND)
        if brand:
            filtered_scif = filtered_scif[filtered_scif['brand_name'] == brand]

        ssd_still = self.does_exist(kpi_line, Const.ATT4)
        if ssd_still:
            filtered_scif = filtered_scif[filtered_scif['manufacturer_name'] == manufacturer]

        size_subpackages = self.does_exist(kpi_line, Const.SIZE_SUBPACKAGES_NUM)
        if size_subpackages:
            # convert all pairings of size and number of subpackages to tuples
            size_subpackages_tuples = [tuple([float(i) for i in x.split(';')]) for x in size_subpackages]
            filtered_scif = filtered_scif[
                pd.Series(list(zip(filtered_scif['size'], filtered_scif['subpackages_num']))).isin(
                    size_subpackages_tuples)]

        sub_packages = self.does_exist(kpi_line, Const.SUBPACKAGES_NUM)
        if sub_packages:
            if sub_packages == Const.NOT_NULL:
                filtered_scif = filtered_scif[~filtered_scif['subpackages_num'].isnull()]
            else:
                filtered_scif = filtered_scif[filtered_scif['subpackages_num'].isin([int(i) for i in sub_packages])]

        if self.does_exist(kpi_line, Const.MINIMUM_FACINGS_REQUIRED):
            number_of_passing_displays = self.get_number_of_passing_displays(filtered_scif)

            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME] + Const.LIBERTY)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                              denominator_id=self.store_id, denominator_result=0,
                                              result=number_of_passing_displays,
                                              identifier_parent=Const.RED_SCORE_PARENT,
                                              should_enter=True)
            return number_of_passing_displays
        else:
            return False

    # Share of Display functions
    def calculate_share_of_display(self, kpi_line, relevant_scif):
        return
        filtered_scif = relevant_scif

        manufacturer = self.does_exist(kpi_line, Const.MANUFACTURER)
        if manufacturer:
            filtered_scif = relevant_scif[relevant_scif['manufacturer_name'] == manufacturer]

        ssd_still = self.does_exist(kpi_line, Const.ATT4)
        if ssd_still:
            filtered_scif = filtered_scif[filtered_scif['manufacturer_name'] == manufacturer]

        if self.does_exist(kpi_line, Const.MARKET_SHARE_TARGET):
            market_share_target = self.get_market_share_target(ssd_still=ssd_still)
        else:
            market_share_target = 0

        # if self.does_exist(kpi_line[Const.INCLUDE_BODY_ARMOR]):
        #     body_armor_scif = relevant_scif[relevant_scif['brand_fk'] == Const.BODY_ARMOR_BRAND_FK]
        #     filtered_scif = filtered_scif.append(body_armor_scif, sort=False)

        if self.does_exist(kpi_line, Const.MINIMUM_FACINGS_REQUIRED):
            number_of_passing_displays = self.get_number_of_passing_displays(filtered_scif)

            result = number_of_passing_displays > market_share_target

            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME] + Const.LIBERTY)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                              denominator_id=self.store_id, denominator_result=0,
                                              result=result, identifier_parent=Const.RED_SCORE_PARENT,
                                              should_enter=True)
            return result
        else:
            return False

    def get_number_of_passing_displays(self, filtered_scif):
        filtered_scif['passed_displays'] = filtered_scif.apply(lambda row: self._calculate_pass_status_of_display(row))

        return filtered_scif['passed_displays'].sum()

    def _calculate_pass_status_of_display(self, row):
        template = self.templates[Const.MINIMUM_FACINGS]
        package_category = (row['size'], row['subpackages_num'], row['unit_of_measure'])
        relevant_template = template[pd.Series(zip(template['size'],
                                                   template['subpackages_num'],
                                                   template['unit_of_measure'])) == package_category]
        minimum_facings = relevant_template[Const.MINIMUM_FACINGS_REQUIRED_FOR_DISPLAY].min()
        return 1 if row['facings'] > minimum_facings else 0

    # Survey functions
    def calculate_survey(self, kpi_line, relevant_scif):
        pass

    # helper functions
    def get_market_share_target(self, ssd_still=None):
        template = self.templates[Const.MARKET_SHARE]
        relevant_template = template[(template[Const.STORE_TYPE] == self.store_type) &
                                     (template[Const.RETAILER] == self.retailer) &
                                     (template[Const.BRANCH] == self.branch)]

        if relevant_template.empty:
            return 0
        if ssd_still:
            if ssd_still.lower() == Const.SSD.lower():
                return relevant_template[Const.SSD].iloc[0]
            elif ssd_still.lower() == Const.STILL.lower():
                return relevant_template[Const.STILL].iloc[0]

        return relevant_template[Const.SSD_AND_STILL].iloc[0]

    def get_body_armor_delivery_status(self):
        list_of_zip_codes = self.templates[Const.BODY_ARMOR][Const.ZIP].unique().tolist()
        store_zip_code = self.store_info['postal_code'].iloc[0]
        if store_zip_code in list_of_zip_codes:
            return 'Y'
        else:
            return 'N'

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
