import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import Shared
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.Const import Const

__author__ = 'Sam'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', Const.TEMPLATE_PATH)
CMA_COMPLIANCE = 'CMA Compliance SW'
MANUFACTURER_FK = 1  # for CCNA


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
        self.scene = self.scene_info.loc[0, 'scene_fk']
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[self.scif['product_type'] != "Irrelevant"]
        self.store_attr = self.store_info['additional_attribute_3'].iloc[0]
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
        self.tools = Shared(self.data_provider, self.output)
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
            template_group = self.does_exist(main_line, Const.TEMPLATE_GROUP)
            template_name = self.does_exist(main_line, Const.SCENE_TYPE)
            store_attrs = self.does_exist(main_line, Const.PROGRAM)
            if (template_group is None or self.scif.loc[0, 'template_group'] in template_group) and\
               (template_name is None or self.scif.loc[0, 'template_name'] in template_name) and\
               (store_attrs is None or self.store_attr in store_attrs):
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
        general_filters = {}

        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        function = self.get_kpi_function(kpi_type)

        for i, kpi_line in relevant_template.iterrows():
            if not relevant_scif.empty:
                result_dict = function(kpi_line, relevant_scif, general_filters)

                # score = Const.PASS if score == 1 else Const.FAIL
                # score = self.tools.result_values[score]
                self.common.write_to_db_result(**result_dict)

    def calculate_coke_cooler_purity(self, kpi_line, scif, general_filters):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = kpi_line[Const.KPI_NAME]

        # num_filters = self.get_kpi_line_filters(kpi_line)
        num_filters = {'Southwest Deliver': 'Y'}
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)

        # scene_scif = scif[scif['scene_fk'] == scene]
        num_scif = scif[self.get_filter_condition(scif, **num_filters)]
        den_scif = scif[self.get_filter_condition(scif, **general_filters)]

        ratio, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)

        competitor_facings = den - num

        # get all brands in the scene
        brand_list = den_scif['brand_name'].unique().tolist()

        for brand in brand_list:
            brand_filters = {'brand_name': brand}
            brand_scif = scif[self.get_filter_condition(scif, **brand_filters)]
            try:
                brand_fk = brand_scif['brand_fk'].values[0]
                southwest_deliver = brand_scif['Southwest Deliver'].values[0] == 'Y'
            except IndexError:
                Log.error(
                    'Foreign key for brand name "{}" not found or bottler delivery status indeterminable'.format(brand))
                continue

            brand_ratio, brand_num, brand_den = self.tools.sos_with_num_and_dem(kpi_line, brand_scif, den_scif,
                                                                                self.facings_field)

            competitor_brand_facings = 0

            if not southwest_deliver:
                competitor_brand_facings = brand_num
                brand_num = 0

            brand_result_dict = self.build_dictionary_for_db_insert(kpi_name=Const.COKE_COOLER_PURITY_BRAND,
                                                                    numerator_result=brand_num,
                                                                    denominator_result=brand_den,
                                                                    result=brand_ratio, score=competitor_brand_facings,
                                                                    numerator_id=brand_fk, denominator_id=self.scene,
                                                                    by_scene=True, should_enter=True,
                                                                    identifier_parent=self.scene)

            self.common.write_to_db_result(**brand_result_dict)

        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_name, numerator_result=num,
                                                          denominator_result=den,
                                                          result=ratio, score=competitor_facings, numerator_id=self.scene,
                                                          denominator_id=MANUFACTURER_FK,
                                                          by_scene=True, should_enter=True,
                                                          identifier_result=self.scene)

        return result_dict

    def calculate_facings_ntba(self, kpi_line, scif, general_filters):
        kpi_name = kpi_line[Const.KPI_NAME]

        targets = self.tools.get_kpi_line_targets(kpi_line)
        facings_filters = self.tools.get_kpi_line_filters(kpi_line)
        score = 0

        facings = scif[self.get_filter_condition(scif, **facings_filters)][self.facings_field].sum()
        num_bays = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == self.scene]['bay_number'].max()
        max_given = max(list(targets.keys()))
        if num_bays in targets:
            target = targets[num_bays]
        else:
            target = None

        if target is None:  # if num bays exceeds doors given in targets, use largest option as target
            target = self.extrapolate_target(targets, max_given)

        if facings >= target:  # Please note, 0 > None evaluates true, so 0 facings is a pass when no target is set
            score = 1

        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_name, numerator_result=facings,
                                                          result=facings, score=score, target=target,
                                                          by_scene=True, should_enter=True)

        # return score, passed, len(scenes)
        return result_dict

    def calculate_ratio(self, kpi_line, scif, general_filters):
        kpi_name = kpi_line[Const.KPI_NAME]

        sos_filters = self.tools.get_kpi_line_filters(kpi_line)
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)
        num_scif = scif[self.get_filter_condition(scif, **sos_filters)]
        den_scif = scif[self.get_filter_condition(scif, **general_filters)]
        score = 0
        target = .8
        if den_scif.empty:
            return None, None, None

        # sos_value = self.shared.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        ratio, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)
        if ratio >= target:
            score = 1

        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_name, numerator_result=num,
                                                          denominator_result=den,
                                                          result=ratio, score=score, target=target,
                                                          by_scene=True, should_enter=True)

        return result_dict

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
        if kpi_type == Const.FACINGS:
            return self.calculate_facings_ntba
        if kpi_type == Const.RATIO:
            return self.calculate_facings_ntba
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
                if ", " in cell:
                    return cell.split(", ")
                else:
                    return cell.split(',')
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

    def build_dictionary_for_db_insert(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                       denominator_id=0, denominator_result=0, score=0, score_after_actions=0,
                                       denominator_result_after_actions=None, numerator_result_after_actions=0,
                                       weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=None,
                                       target=None, identifier_parent=None, identifier_result=None, should_enter=None,
                                       by_scene=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.common.get_kpi_fk_by_kpi_type('{} {}'.format(CMA_COMPLIANCE, kpi_name))
            else:
                insert_params['fk'] = fk
            insert_params['numerator_id'] = numerator_id
            insert_params['numerator_result'] = numerator_result
            insert_params['denominator_id'] = denominator_id
            insert_params['denominator_result'] = denominator_result
            insert_params['result'] = result
            insert_params['score'] = score
            if target:
                insert_params['target'] = target
            if denominator_result_after_actions:
                insert_params['denominator_result_after_actions'] = denominator_result_after_actions
            if context_id:
                insert_params['context_id'] = context_id
            if identifier_parent:
                insert_params['identifier_parent'] = identifier_parent
            if should_enter:
                insert_params['should_enter'] = should_enter
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            if by_scene:
                insert_params['by_scene'] = by_scene  # needed specifically for the modified Common.py this project uses
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None
