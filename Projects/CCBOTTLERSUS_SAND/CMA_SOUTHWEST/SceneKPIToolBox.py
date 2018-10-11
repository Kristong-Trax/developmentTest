import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import sos_with_num_and_dem
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.Const import Const

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', Const.TEMPLATE_PATH)

class CCBOTTLERSUS_SANDSceneCokeCoolerToolbox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
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
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        if self.store_type in Const.STORE_TYPE:
            self.store_type = Const.STORE_TYPE[self.store_type]
        self.templates = {}
        for sheet in Const.SHEETS_CMA:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.region = self.store_info['region_name'].iloc[0]
        main_template = self.templates[Const.KPIS]
        self.templates[Const.KPIS] = main_template[(main_template['Regions'] == self.region) &
                                                   (main_template[Const.STORE_TYPE] == self.store_type)]
        # self.scenes_results = pd.DataFrame(columns=Const.COLUMNS_OF_RESULTS)

    def main_calculation(self):
        """
            This function makes the calculation for the scene's KPI and returns their answers to the session's calc
        """
        if self.scif[self.scif['Southwest Deliver'] == 'Y'].empty:  # if it's not sw scene we don't need to calculate
            return False
        main_template = self.templates[Const.KPIS]
        main_template = main_template[main_template[Const.SESSION_LEVEL] != 'Y']
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        # self.write_results_to_db()
        return True

    def write_results_to_db(self):
        """
        Now we are just writing all the scene results to db
        """
        for i, scene_result in self.scenes_results.iterrows():
            self.common.write_to_db_result(
                fk=self.common.get_kpi_fk_by_kpi_name(scene_result[Const.KPI_NAME] + Const.SCENE_SUFFIX),
                result=round(scene_result[Const.DB_RESULT], 2), by_scene=True)

    def write_to_scene_level(self, kpi_name, result=False, parent=""):
        """
        Writes a result in the DF (and "tells" its parent if it passed)
        :param kpi_name: string
        :param result: boolean
        :param parent: if the kpi is a condition kpi and it passed, we want the parent to know that
                        because we want the kpi to choose the scene with the most passed children
        """
        if parent and result:
            self.scenes_results.loc[(self.scenes_results[Const.KPI_NAME] == parent) &
                                    (self.scenes_results[Const.DB_RESULT] > 0), Const.DB_RESULT] += 1
        result_dict = {Const.KPI_NAME: kpi_name, Const.DB_RESULT: result * 1}
        # self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        relevant_scif = self.scif.copy()
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        store_attrs = self.does_exist(main_line, Const.PROGRAM)
        general_filters = {}

        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        scene_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            general_filters['template_group'] = scene_groups

        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        function = self.get_kpi_function(kpi_type)

        for i, kpi_line in relevant_template.iterrows():
            if (store_attrs is not None and self.store_attr not in store_attrs)\
                    or relevant_scif.empty:
                continue
            function(kpi_line, relevant_scif, general_filters)
        else:
            pass

    def calculate_coke_cooler_purity(self, kpi_line, scif, general_filters):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_line['KPI name'])

        # num_filters = self.get_kpi_line_filters(kpi_line)
        num_filters = {'Southwest Deliver': 'Y'}
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)

        # scene_scif = scif[scif['scene_fk'] == scene]
        num_scif = scif[self.get_filter_condition(scif, **num_filters)]
        den_scif = scif[self.get_filter_condition(scif, **general_filters)]

        ratio, num, den = sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)

        self.common.write_to_db_result(fk=kpi_fk, numerator_result=num, denominator_result=den,
                                           result=ratio, by_scene=True,
                                           # identifier_parent=self.common.get_dictionary(
                                           #     parent_name='Total Coke Cooler Purity'),
                                           should_enter=True)

        # self.common.write_to_db_result(fk=2161, numerator_result=total_num,
        #                                    denominator_result=total_den, result=ratio,
        #                                    should_enter=True, by_scene=True)

        # self.common_scene.write_to_db_result(fk=2161, numerator_result=total_num,
        #                                    denominator_result=total_den, result=ratio,
        #                                    identifier_parent=self.common_db2.get_dictionary(
        #                                        parent_name='CMA_COMPLIANCE'),
        #                                    should_enter=True)
        # self.common_scene.commit_results_data(result_entity='scene')


        # scene_types = self.toolbox.does_exist(main_line, Const.SCENE_TYPE)
        # scene_groups = self.toolbox.does_exist(main_line, Const.SCENE_TYPE_GROUP)
        # if (scene_types and self.scif['template_name'].iloc[0] not in scene_types) or \
        #         (scene_groups and self.scif['template_group'].iloc[0] not in scene_groups):
        #     return
        # kpi_name = main_line[Const.KPI_NAME]
        # parent = main_line[Const.CONDITION]
        # result = self.toolbox.calculate_kpi_by_type(main_line, self.scif)
        # if result is not None:
        #     self.write_to_scene_level(
        #         kpi_name=kpi_name, result=result, parent=parent)

    def session_kpi(self, kpi_fk):
        results = self.data_provider[Data.SCENE_KPI_RESULTS]
        kpi_res = results[results['kpi_level_2_fk'] == kpi_fk]
        num = kpi_res['numerator_result'].sum()
        den = kpi_res['denominator_result'].sum()

        if den:
            ratio = float(num) / den
        else:
            ratio = 0
        self.common.kpi_results = pd.DataFrame(columns=self.common.COLUMNS)
        self.common.write_to_db_result(fk=2161, numerator_result=num, denominator_result=den,
                                       result=ratio, numerator_id=1,
                                       denominator_id=self.store_id,
                                       # identifier_result=self.common.get_dictionary(
                                       #     parent_name='Total Coke Cooler Purity'),
                                       # should_enter=True
                                       )
        self.common.commit_results_data()

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """

        if kpi_type == Const.PURITY:
            return self.calculate_coke_cooler_purity
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
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
