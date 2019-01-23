from datetime import datetime
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.MSC.Data.Const import Const
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Elyashiv'


class MSCToolBox:

    def __init__(self, data_provider, output, calculation_type, common_db):
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
        self.calculation_type = calculation_type
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.common_db = common_db
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]


    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        if not main_template.empty:
            pass
        return

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        relevant_scif = self.scif
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        result = self.calculate_kpi_by_type(main_line, relevant_scif)
        # self.write_to_session_level(kpi_name=kpi_name, result=result)
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
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME]
                                              == main_line[Const.KPI_NAME]]
        kpi_function = self.get_kpi_function(kpi_type)

        return self.calculate_specific_kpi(relevant_template, filtered_scif, kpi_function)

    @staticmethod
    def calculate_specific_kpi(relevant_template, filtered_scif, kpi_function, target=None):
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
            answer = kpi_function(kpi_line, filtered_scif)
            if answer:
                passed_counter += 1
            elif answer is None:
                return None
        return passed_counter >= target

    # availability calculations
    def calculate_availability(self, kpi_line, relevant_scif):  # V
        """
        checks if all the lines in the availability sheet passes the KPI (there is at least one product
        in this relevant scif that has the attributes).
        :param relevant_scif: filtered scif
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we shouldn't calculate
        DP lines
        :param kpi_line: line from the availability sheet
        :return: boolean
        """
        filtered_scif = self.filter_scif_availability(kpi_line, relevant_scif)
        minimum_facings = kpi_line[Const.MINIMUM_FACINGS]
        return filtered_scif[filtered_scif['facings'] > 0]['facings'].count() >= minimum_facings

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
            Const.PREMIUM_SSD: "Premium SSD",
            Const.INNOVATION_BRAND: "Innovation Brand",
        }
        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(
                relevant_scif, kpi_line, name, names_of_columns[name])
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

    # helper functions
    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == Const.DOUBLE_AVAILABILITY:
            return self.calculate_double_availability
        elif kpi_type == Const.PRESENCE:
            return self.calculate_presence
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
                return cell.split(", ")
        return None
