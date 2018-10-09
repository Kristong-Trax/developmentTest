import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.Const import Const

__author__ = 'Elyashiv'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Southwest CMA Compliance Template_v8.xlsx')

class CCBOTTLERSUS_SANDSceneCokeCoolerToolbox:

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
        main_template = main_template[main_template[Const.SESSION_LEVEL] != Const.V]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.write_results_to_db()
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
        self.scenes_results = self.scenes_results.append(result_dict, ignore_index=True)

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
            result, score, target = function(kpi_line, relevant_scif, general_filters)

            # write in DF:
            if result is None and score is None and target is None:
                continue
            print(kpi_name, kpi_type, result, score, target)
        else:
            pass

    def calculate_coke_cooler_purity(self, kpi_line, scif, general_filters):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet, scene after scene.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_line['KPI name'])

        # num_filters = self.get_kpi_line_filters(kpi_line)
        num_filters = {'Southwest Deliver': 'Y'}
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)

        # scene_scif = scif[scif['scene_fk'] == scene]

        num, ratio, den = self.sos_with_num_and_dem(kpi_line, scif, num_filters, general_filters)

        self.common.write_to_db_result(fk=2161, numerator_result=total_num,
                                           denominator_result=total_den, result=ratio,
                                           should_enter=True, by_scene=True)

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

    def sos_with_num_and_dem(self, kpi_line, relevant_scif, num_filters,  general_filters):

        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **num_filters)]
        den_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]

        try:
            Validation.is_empty_df(den_scif)
            Validation.is_empty_df(num_scif)
            Validation.df_columns_equality(den_scif, num_scif)
            Validation.is_subset(den_scif, num_scif)
        except Exception, e:
            msg = "Data verification failed: {}.".format(e)
            raise Exception(msg)
        num = num_scif[self.facings_field].sum()
        den = den_scif[self.facings_field].sum()

        ratio = num / float(den)
        # numerator_id=product_fk,
        # self.common.write_to_db_result(fk=kpi_fk, numerator_result=num, denominator_result=den,
        #                                result=ratio, by_scene=True)

        # self.common.write_to_db_result(fk=kpi_fk, numerator_result=num,
        #                                    denominator_result=den, result=ratio, by_scene=True,
        #                                    identifier_parent=self.common_db2.get_dictionary(
        #                                        parent_name='Total Coke Cooler Purity'),
        #                                    should_enter=True)
        return num, ratio, den

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
