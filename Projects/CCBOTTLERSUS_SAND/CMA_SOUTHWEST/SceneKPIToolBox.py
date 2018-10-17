import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import Shared
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.Const import Const

__author__ = 'Sam'

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
        self.scene = self.scene_info[0]
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
        self.tools = Shared()
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
            num, den, result, score, target = function(kpi_line, relevant_scif, general_filters)
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_line['KPI name'])

            self.common.write_to_db_result(fk=kpi_fk, numerator_result=num, denominator_result=den,
                                           result=result, score=score, target=target,  by_scene=True, should_enter=True)

    def calculate_coke_cooler_purity(self, kpi_line, scif, general_filters):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """

        # num_filters = self.get_kpi_line_filters(kpi_line)
        num_filters = {'Southwest Deliver': 'Y'}
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)

        # scene_scif = scif[scif['scene_fk'] == scene]
        num_scif = scif[self.get_filter_condition(scif, **num_filters)]
        den_scif = scif[self.get_filter_condition(scif, **general_filters)]

        ratio, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)
        return num, den, ratio, None, None


    def calculate_facings_ntba(self, kpi_line, scif, general_filters):
        targets = self.tools.get_kpi_line_targets(kpi_line)
        facings_filters = self.get_kpi_line_filters(kpi_line)
        score = 0

        facings = scif[self.get_filter_condition(scif, **facings_filters)][self.facings_field].sum()
        num_bays = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == self.scene]['bay_number'].max()
        max_given = max(list(targets.keys()))
        print('Num bays is', num_bays)
        if num_bays in targets:
            target = targets[num_bays]
        else:
            target = None

        if target is None:  # if num bays exceeds doors given in targets, use largest option as target
            target = self.extrapolate_target(targets, max_given)

        if facings >= target:  # Please note, 0 > None evaluates true, so 0 facings is a pass when no target is set
            score = 1

        # return score, passed, len(scenes)
        return facings, None, facings, score, target

    @staticmethod
    def extrapolate_target(targets, c):
        while 1:
            if targets[c]:
                target = targets[c]
                break
            else:
                c -= 1
                if c < 0:
                    target = 0
                    break
        return target

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