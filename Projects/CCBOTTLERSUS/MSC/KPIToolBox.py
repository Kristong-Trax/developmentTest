from datetime import datetime
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.MSC.Data.Const import Const
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Hunter'


class MSCToolBox:

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
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.manufacturer_fk = Const.MANUFACTURER_FK


    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            relevant_store_types = self.does_exist(main_line, Const.STORE_TYPE)
            if relevant_store_types and self.store_type not in relevant_store_types:
                continue
            self.calculate_main_kpi(main_line)
        if len(self.common_db.kpi_results) > 0:
            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(Const.MSC)
            self.common_db.write_to_db_result(kpi_fk, numerator_id=1, denominator_id=self.store_id, result=1,
                                              identifier_result=Const.MSC, should_enter=True)
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

    # facings calculations
    def calculate_facings(self, kpi_line, relevant_scif):
        denominator_scif = self.get_denominator_scif(kpi_line, relevant_scif)
        numerator_scif = self.get_numerator_scif(kpi_line, denominator_scif)

        numerator_result = numerator_scif['facings'].sum()
        denominator_result = denominator_scif['facings'].sum()
        sos_value = numerator_result / denominator_result

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=numerator_result,
                                          denominator_id=self.store_id, denominator_result=denominator_result,
                                          result=sos_value * 100, identifier_parent=Const.MSC, should_enter=True)

        return

    def check_activation_status(self, kpi_line, relevant_scif):
        """
        This function checks to see whether or not the KPI has an activation parameter and value combo defined.
        If it does, the function makes sure that ALL values are present
        :param kpi_line:
        :param relevant_scif:
        :return:
        """


        try:
            activation_param = kpi_line[Const.ACTIVATION_TYPE]
        except KeyError:
            activation_param = None
        if activation_param:
            relevant_scif = self.filter_scif_by_template_columns(kpi_line, Const.ACTIVATION_TYPE,
                                                                 Const.ACTIVATION_VALUE, relevant_scif)

            if relevant_scif.empty:
                return False
            else:
                return True
        else:
            # no activation for this KPI? return true
            return True

    def calculate_availability(self, kpi_line, relevant_scif, save_result=True):
        availability = True
        numerator_scif = self.get_numerator_scif(kpi_line, relevant_scif)

        if numerator_scif.empty:
            availability = False

        minimum_skus = self.does_exist(kpi_line, Const.MINIMUM_SKUS)
        if minimum_skus:
            number_of_skus = len(numerator_scif['product_name'].unique())
            availability = number_of_skus > minimum_skus[0]

        minimum_brands = self.does_exist(kpi_line, Const.MINIMUM_BRANDS)
        if minimum_brands:
            number_of_brands = len(numerator_scif['brand_name'].unique())
            availability = number_of_brands > minimum_brands[0]

        minimum_packages = self.does_exist(kpi_line, Const.MINIMUM_PACKAGES)
        if minimum_packages:
            number_of_packages = len(numerator_scif.drop_duplicates(subset=['Multi-Pack Size', 'Base Size']))
            availability = number_of_packages > minimum_packages[0]

        threshold = self.does_exist(kpi_line, Const.THRESHOLD)
        if threshold:
            availability = numerator_scif['facings'].sum() / relevant_scif['facings'].sum() > threshold[0]

        # result = self.ps_data_provider.get_pks_of_result(
        #     Const.PASS) if availability else self.ps_data_provider.get_pks_of_result(Const.FAIL)
        result = 100 if availability else 0

        if save_result:
            kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
            self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                              result=result, identifier_parent=Const.MSC, should_enter=True)
        return availability

    def calculate_per_scene_availability(self, kpi_line, relevant_scif):
        scenes = relevant_scif['scene_fk'].unique().tolist()
        passing_scenes = 0

        for scene in scenes:
            scene_scif = relevant_scif[relevant_scif['scene_fk'] == scene]

            if not self.check_activation_status(kpi_line, scene_scif):
                continue

            passing_scenes += self.calculate_availability(kpi_line, scene_scif, save_result=False)
            if passing_scenes:  # we only need at least one passing scene
                break

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                          result=passing_scenes, identifier_parent=Const.MSC, should_enter=True)

        return passing_scenes

    def calculate_double_availability(self, kpi_line, relevant_scif):
        group_1_scif = self.filter_scif_availability(kpi_line, relevant_scif, group=1)
        group_1_minimum_facings = kpi_line[Const.GROUP1_MINIMUM_FACINGS]
        if not group_1_scif['facings'].sum() >= group_1_minimum_facings:
            return False

        group_2_scif = self.filter_scif_availability(kpi_line, relevant_scif, group=2)
        group_2_minimum_facings = kpi_line[Const.GROUP2_MINIMUM_FACINGS]
        availability = group_2_scif['facings'].sum() >= group_2_minimum_facings

        # result = self.ps_data_provider.get_pks_of_result(
        #     Const.PASS) if availability else self.ps_data_provider.get_pks_of_result(Const.FAIL)
        result = 100 if availability else 0

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                          result=result, identifier_parent=Const.MSC, should_enter=True)

        return availability

    def filter_scif_availability(self, kpi_line, relevant_scif, group=None):
        """
        calls filter_scif_specific for every column in the template of availability
        :param kpi_line:
        :param relevant_scif:
        :param group: used to indicate group for double availability
        :return:
        """
        if group == 1:
            names_of_columns = {
                Const.GROUP1_BRAND: "brand_name",
            }
        elif group == 2:
            names_of_columns = {
                Const.GROUP2_BRAND: "brand_name",
            }
        else:
            return relevant_scif

        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(
                relevant_scif, kpi_line, name, names_of_columns[name])

        return relevant_scif

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

    # Share of Scenes functions
    def calculate_share_of_scenes(self, kpi_line, relevant_scif):
        relevant_scenes_scif = self.get_denominator_scif(kpi_line, relevant_scif)
        # we need to get only the scenes from the denominator scif
        denominator_scif = \
            relevant_scif[relevant_scif['scene_fk'].isin(relevant_scenes_scif['scene_fk'].unique().tolist())]

        # the numerator population is applied only to scenes that exist in the denominator population
        numerator_scif = self.get_numerator_scif(kpi_line, denominator_scif)

        agg_denominator_scif = denominator_scif.groupby('scene_fk', as_index=False)[['facings']].sum()
        agg_denominator_scif.rename(columns={'facings': 'den_facings'}, inplace=True)
        agg_numerator_scif = numerator_scif.groupby('scene_fk', as_index=False)[['facings']].sum()
        agg_numerator_scif.rename(columns={'facings': 'num_facings'}, inplace=True)

        results = agg_numerator_scif.merge(agg_denominator_scif)
        results['sos'] = (results['num_facings'] / results['den_facings'])
        results['sos'].fillna(0, inplace=True)

        threshold = self.does_exist(kpi_line, Const.THRESHOLD)
        if threshold:
            results = results[results['sos'] > threshold[0]]

        numerator_scenes = len(results[results['sos'] > 0])
        denominator_scenes = len(results)

        if denominator_scenes > 0:
            sos_value = numerator_scenes / float(denominator_scenes)
        else:
            sos_value = 0

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=numerator_scenes,
                                          denominator_id=self.store_id, denominator_result=denominator_scenes,
                                          result=sos_value * 100, identifier_parent=Const.MSC, should_enter=True)
        return

    # Share of POCs functions
    def calculate_share_of_pocs(self, kpi_line, relevant_scif):
        numerator_scif = self.get_numerator_scif(kpi_line, relevant_scif)
        denominator_scif = self.get_denominator_scif(kpi_line, relevant_scif)

        minimum_facings = self.does_exist(kpi_line, Const.MINIMUM_FACINGS)
        if minimum_facings:
            numerator_scif = numerator_scif[numerator_scif['facings'] >= minimum_facings[0]]
            denominator_scif = denominator_scif[denominator_scif['facings'] >= minimum_facings[0]]

        numerator_scenes = len(numerator_scif['scene_fk'].unique())
        denominator_scenes = len(denominator_scif['scene_fk'].unique())

        poc_share = numerator_scenes / float(numerator_scenes + denominator_scenes)

        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type(kpi_line[Const.KPI_NAME])
        self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=numerator_scenes,
                                          denominator_id=self.store_id, denominator_result=denominator_scenes,
                                          result=poc_share * 100, identifier_parent=Const.MSC, should_enter=True)

        return

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
        elif kpi_type == Const.PER_SCENE_AVAILABILITY:
            return self.calculate_per_scene_availability
        elif kpi_type == Const.FACINGS:
            return self.calculate_facings
        elif kpi_type == Const.SHARE_OF_SCENES:
            return self.calculate_share_of_scenes
        elif kpi_type == Const.SHARE_OF_POCS:
            return self.calculate_share_of_pocs
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

    def get_numerator_scif(self, kpi_line, denominator_scif):
        numerator_scif = self.filter_scif_by_template_columns(kpi_line, Const.NUM_TYPE, Const.NUM_VALUE,
                                                              denominator_scif)
        numerator_scif = self.filter_scif_by_template_columns(kpi_line, Const.NUM_EXCLUDE_TYPE, Const.NUM_EXCLUDE_VALUE,
                                                              numerator_scif, exclude=True)
        numerator_scif = self.filter_scif_by_template_columns(kpi_line, Const.EXCLUDED_TYPE, Const.EXCLUDED_VALUE,
                                                              numerator_scif, exclude=True)
        return numerator_scif

    def get_denominator_scif(self, kpi_line, relevant_scif):
        denominator_scif = self.filter_scif_by_template_columns(kpi_line, Const.DEN_TYPE, Const.DEN_VALUE,
                                                                relevant_scif)
        denominator_scif = self.filter_scif_by_template_columns(kpi_line, Const.EXCLUDED_TYPE, Const.EXCLUDED_VALUE,
                                                                denominator_scif, exclude=True)
        return denominator_scif

    @staticmethod
    def filter_scif_by_template_columns(kpi_line, type_base, value_base, relevant_scif, exclude=False):
        filters = {}

        # get denominator filters
        for den_column in [col for col in kpi_line.keys() if type_base in col]:  # get relevant den columns
            if kpi_line[den_column]:  # check to make sure this kpi has this denominator param
                filters[kpi_line[den_column]] = \
                    [value.strip() for value in kpi_line[den_column.replace(type_base, value_base)].split(
                        ',')]  # get associated values

        for key in filters.iterkeys():
            if key not in relevant_scif.columns.tolist():
                Log.error('{} is not a valid parameter type'.format(key))
                continue
            if exclude:
                relevant_scif = relevant_scif[~(relevant_scif[key].isin(filters[key]))]
            else:
                relevant_scif = relevant_scif[relevant_scif[key].isin(filters[key])]

        return relevant_scif

