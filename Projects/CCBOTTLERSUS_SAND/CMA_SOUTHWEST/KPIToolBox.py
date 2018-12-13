import os
from datetime import datetime
import pandas as pd
import numpy as np
from collections import defaultdict
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.Const import Const
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import Shared
from Projects.CCBOTTLERSUS_SAND.SCENE_SESSION.CommonV3 import Common as CommonV3
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS




__author__ = 'Uri'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', Const.TEMPLATE_PATH)
SURVEY_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SurveyTemplateV2.xlsx')
############
STORE_TYPES = {
    "CR SOVI RED": "CR&LT",
    "DRUG SOVI RED": "Drug",
    "VALUE SOVI RED": "Value",
    "United Test - Value SOVI RED": "Value",
    "United Test - Drug SOVI RED": "Drug",
    "United Test - CR SOVI RED": "CR&LT",
    "FSOP - QSR": "QSR",
}
SUB_PROJECT = 'CMA Compliance SW'


class CMASOUTHWESTToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, common_db2):
        self.output = output
        self.data_provider = data_provider
        # self.common_db = Common(self.data_provider, SUB_PROJECT)
        # self.common_db2 = common_db2
        self.common_db2 = CommonV3(self.data_provider)
        self.common_scene = CommonV2(self.data_provider)
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
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.sw_scenes = self.get_sw_scenes() # we don't need to check scenes without United products
        self.survey = Survey(self.data_provider, self.output)
        self.sos = SOS(self.data_provider, self.output)
        self.results = self.data_provider[Data.SCENE_KPI_RESULTS]
        self.templates = {}
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        self.sales_center = self.store_info['additional_attribute_5'].iloc[0]
        if self.store_type in STORE_TYPES: #####
            self.store_type = STORE_TYPES[self.store_type] ####
        self.store_attr = self.store_info['additional_attribute_3'].iloc[0]
        # self.kpi_static_data = self.common_db.get_kpi_static_data()
        self.total_score = 0
        self.sub_scores = defaultdict(int)
        self.sub_totals = defaultdict(int)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
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
        main_template = main_template[main_template[Const.SESSION_LEVEL] == 'Y']
        if self.region in Const.REGIONS:
            for i, main_line in main_template.iterrows():
                store_type = self.does_exist(main_line, Const.STORE_TYPE)
                if store_type is None or self.store_type in store_type:
                    self.calculate_main_kpi(main_line)
            self.write_scene_parent()
            self.write_sub_parents()
            self.write_parent()
            # self.write_to_db_result(
            #     self.common_db.get_kpi_fk_by_kpi_name(SUB_PROJECT, 1), score=self.total_score, level=1)

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        if kpi_name not in Const.ALL_SCENE_KPIS:  # placeholder- need to check for unintended consequences
            relevant_scif = self.scif[self.scif['scene_id'].isin(self.sw_scenes)]
        else:
            relevant_scif = self.scif.copy()
        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        scene_level = self.does_exist(main_line, Const.SCENE_LEVEL)
        store_attrs = main_line[Const.PROGRAM].split(',')
        result = score = target = None
        general_filters = {}

        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        scene_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            general_filters['template_group'] = scene_groups

        if kpi_type == 'shelves bonus':
            relevant_template = self.templates['shelves']
        else:
            relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        function = self.get_kpi_function(kpi_type)

        for i, kpi_line in relevant_template.iterrows():
            if not self.store_attr or (store_attrs[0] != '' and self.store_attr not in store_attrs)\
                    or relevant_scif.empty:
                continue
            result, score, target = function(kpi_line, relevant_scif, general_filters)
            if result is None and score is None and target is None:
                continue
            self.update_parents(kpi_name, result, score)
            if isinstance(result, tuple):
                self.write_to_all_levels(kpi_name=kpi_name, result=result[0], score=score, target=target,
                                         num=result[1], den=result[2])
            else:
                self.write_to_all_levels(kpi_name=kpi_name, result=result, score=score, target=target)
        else:
            pass

    def write_to_session_level(self, kpi_name, result=0):
        """
        Writes a result in the DF
        :param kpi_name: string
        :param result: boolean
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result * 1}
        self.session_results = self.session_results.append(result_dict, ignore_index=True)

    def write_to_all_levels(self, kpi_name, result, score, target=None, scene_fk=None, reuse_scene=False,
                            num=None, den=None):
        """
        Writes the final result in the "all" DF, add the score to the red score and writes the KPI in the DB
        :param kpi_name: str
        :param result: int
        :param display_text: str
        :param weight: int/float
        :param scene_fk: for the scene's kpi
        :param reuse_scene: this kpi can use scenes that were used
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.RESULT: result, Const.SCORE: score, Const.THRESHOLD: target}
        # self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.write_to_db(kpi_name, score, result=result, threshold=target, num=num, den=den)

    # availability:

    def calculate_availability(self, kpi_line, relevant_scif):
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
            # CCBOTTLERSUSConst.PREMIUM_SSD: "Premium SSD",
            # CCBOTTLERSUSConst.INNOVATION_BRAND: "Innovation Brand",
        }
        for name in names_of_columns:
            relevant_scif = self.filter_scif_specific(relevant_scif, kpi_line, name, names_of_columns[name])
        return relevant_scif

    # SOS:

    def calculate_sos(self, kpi_line, relevant_scif, general_filters):
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        # relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)
        relevant_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        if kpi_line['range'] == 'Y':
            upper_limit, lower_limit = self.get_sos_targets(kpi_name, sos_range=True)
            target = None
        else:
            upper_limit, lower_limit = None, None
            target = self.get_sos_targets(kpi_name)

        sos_filters = self.get_kpi_line_filters(kpi_line, name='numerator')
        general_filters = self.get_kpi_line_filters(kpi_line, name='denominator')

        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **sos_filters)]
        den_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        sos_value, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)

        if sos_value is None:
            return None, None, None
        if target:
            target *= 100
            score = 1 if sos_value >= target else 0
            target = '{}%'.format(int(target))
        elif not target and upper_limit and lower_limit:
            score = 1 if (lower_limit <= sos_value <= upper_limit) else 0
            target = '{}% - {}%'.format(lower_limit, upper_limit)
        else:
            score = 0
            target = None
        return (sos_value, num, den), score, target

    # Targets:
    def get_sos_targets(self, kpi_name, sos_range=False):
        targets_template = self.templates[Const.TARGETS]
        store_targets = targets_template.loc[(targets_template[Const.PROGRAM] == self.program) &
                                             (targets_template['region'] == self.region)]
        filtered_targets_to_kpi = store_targets.loc[targets_template['KPI name'] == kpi_name]
        if sos_range:
            if not filtered_targets_to_kpi.empty:
                range = filtered_targets_to_kpi['target'].values[0].split(' - ')
                upper_limit = int(range[1].replace('%', '').strip())
                lower_limit = int(range[0].replace('%', '').strip())
            else:
                upper_limit, lower_limit = None, None
            return upper_limit, lower_limit
        else:
            if not filtered_targets_to_kpi.empty:
                target = float(filtered_targets_to_kpi[Const.TARGET].values[0])
            else:
                target = None
            return target


    def get_targets(self, kpi_name):
        targets_template = self.templates[Const.TARGETS]
        store_targets = targets_template.loc[(targets_template[Const.PROGRAM] == self.program) &
                                             (targets_template['region'] == self.region)]
        filtered_targets_to_kpi = store_targets.loc[targets_template['KPI name'] == kpi_name]
        if not filtered_targets_to_kpi.empty:
            target = filtered_targets_to_kpi[Const.TARGET].values[0]
        else:
            target = None
        return target

    @staticmethod
    def get_kpi_line_filters(kpi_line, name=''):
        if name:
            name = name.lower() + ' '
        filters = defaultdict(list)
        attribs = [x.lower() for x in kpi_line.index]
        kpi_line.index = attribs
        c = 1
        while 1:
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)]:
                filters[kpi_line['{}param {}'.format(name, c)]] += (kpi_line['{}value {}'.format(name, c)].split(','))
            else:
                if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
                    break
            c += 1
        return filters

    @staticmethod
    def get_kpi_line_targets(kpi_line):
        mask = kpi_line.index.str.contains('Target')
        if mask.any():
            targets = kpi_line.loc[mask].replace('', np.nan).dropna()
            targets.index = [int(x.split(Const.SEPERATOR)[1].split(' ')[0]) for x in targets.index]
            targets = targets.to_dict()
        else:
            targets = {}
        return targets

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

    def calculate_facings_ntba(self, kpi_line, relevant_scif, general_filters):
        # if not self.store_attr in kpi_line[Const.PROGRAM].split(','):
        #     return 0, 0, 0

        scenes = relevant_scif['scene_fk'].unique().tolist()
        targets = self.get_kpi_line_targets(kpi_line)
        facings_filters = self.get_kpi_line_filters(kpi_line)
        score = 0
        passed = 0
        sum_facings = 0
        sum_target = 0

        for scene in scenes:
            scene_scif = relevant_scif[relevant_scif['scene_fk'] == scene]
            facings = scene_scif[self.get_filter_condition(scene_scif, **facings_filters)][self.facings_field].sum()
            num_bays = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]['bay_number'].max()
            max_given = max(list(targets.keys()))
            print('Num bays is', num_bays)
            if num_bays in targets:
                target = targets[num_bays]
            else:
                target = None

            if target is None:  # if num bays exceeds doors given in targets, use largest option as target
                target = self.extrapolate_target(targets, max_given)

            if facings >= target:  # Please note, 0 > None evaluates true, so 0 facings is a pass when no target is set
                score += 1
            sum_facings += facings
            sum_target += target

        if score == len(scenes):
            passed = 1

        # return score, passed, len(scenes)
        return score, passed, len(scenes)


    def calculate_ratio(self, kpi_line, relevant_scif, general_filters):
        sos_filters = self.get_kpi_line_filters(kpi_line)
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)
        scenes = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]['scene_fk'].unique().tolist()
        us = 0
        them = 0
        if not scenes:
            return None, None, None

        for scene in scenes:
            sos_filters['scene_fk'] = scene
            sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
            if sos_value >= .8:
                us += 1
            else:
                them += 1

        passed = 0
        if us - them >= 0:
            passed = 1

        if them != 0:
            score = round((us/float(them))*100, 2)
        elif us > 0:
            score = us
        else:
            score = 0
        target = us + them
        if target != 1:
            target = round(((us + them) / 2) * 100, 2)

        return (score, us, them), passed, 100

    def calculate_number_of_shelves(self, kpi_line, relevant_scif, general_filters):
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
        relevant_scenes = relevant_scif['scene_fk'].unique().tolist()
        numerator_filters = self.get_kpi_line_filters(kpi_line)
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)
        scene_filters = {'scene_fk': relevant_scenes}
        target = self.get_targets(kpi_name)
        if isinstance(target, unicode):
            target = str(target)
        if isinstance(target, str):
            target = float(target.split(' ')[0].strip())

        numerator_facings = relevant_scif[self.get_filter_condition(relevant_scif, **numerator_filters)][
            self.facings_field].sum()
        denominator_facings = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)][
            self.facings_field].sum()
        # general_filters['Southwest Deliver'] = 'Y'
        # number_of_shelves_value = self.match_product_in_scene[self.get_filter_condition(
        #     self.match_product_in_scene, **general_filters)][['scene_fk', 'bay_number', 'shelf_number']].\
        #     unique().count()
        number_of_shelves_value = self.match_product_in_scene[self.get_filter_condition(
                                        self.match_product_in_scene, **scene_filters)]\
                                        [['scene_fk', 'bay_number', 'shelf_number']]\
                                        .drop_duplicates().shape[0]

        number_of_shelves_score = numerator_facings / float(denominator_facings / float(number_of_shelves_value))

        if target:
            score = 1 if number_of_shelves_score >= target else 0
        else:
            score = 1
            target = 0

        if 'bonus' not in kpi_name.lower():
            return number_of_shelves_score, score, target
        elif not kpi_line[Const.TARGET]:
            return score, None, None
        else:
            return number_of_shelves_score, None, None

    def write_scene_parent(self):
        self.results['parent_kpi'] = [int(Const.SCENE_SESSION_KPI[kpi]) if kpi in Const.SCENE_SESSION_KPI else None
                                      for kpi in self.results['kpi_level_2_fk']]
        self.results = self.results[~self.results['parent_kpi'].isnull()]
        for i, parent_kpi in enumerate(set(self.results['parent_kpi'])):
            kpi_res = self.results[self.results['parent_kpi'] == parent_kpi]
            num, den, score = self.aggregate(kpi_res, parent_kpi)

            parent_name = self.common_db2.kpi_static_data.set_index('pk').loc[parent_kpi, 'type']
            self.sub_totals[parent_name] = den
            self.sub_scores[parent_name] = num

            self.write_hierarchy(kpi_res, i, parent_name)

    def write_hierarchy(self, kpi_res, i, parent_name):
        for j, kpi_line in kpi_res.iterrows():
            kpi_fk = kpi_line['scene_kpi_fk']
            self.common_db2.write_to_db_result(0, parent_fk=i, scene_result_fk=kpi_fk, should_enter=True,
                                               identifier_parent=self.common_db2.get_dictionary(
                                               parent_name=parent_name), hierarchy_only=1,
                                               numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id)

    def aggregate(self, kpi_res, parent_kpi):
        if Const.BEHAVIOR[parent_kpi] == 'PASS':
            num = kpi_res['score'].sum()
            den = kpi_res['parent_kpi'].count()

        else:
            num = kpi_res['numerator_result'].sum()
            den = kpi_res['denominator_result'].sum()

        score = kpi_res['score'].sum()
        return num, den, score

    # Number of shelves
    def old_calculate_number_of_shelves(self, kpi_line, relevant_scif, general_filters):
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        kpi_name = kpi_line[Const.KPI_NAME]
        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
        den_type = kpi_line[Const.DEN_TYPES_1]
        den_value = kpi_line[Const.DEN_VALUES_1].split(',')
        # relevant_scif = self.filter_by_type_value(relevant_scif, den_type, den_value)
        num_type = kpi_line[Const.NUM_TYPES_1]
        num_value = kpi_line[Const.NUM_VALUES_1].split(',')
        # num_scif = self.filter_by_type_value(relevant_scif, num_type, num_value)
        target = self.get_sos_targets(kpi_name)
        general_filters[den_type] = den_value
        if kpi_line[Const.DEN_TYPES_2]:
            den_type_2 = kpi_line[Const.DEN_TYPES_2]
            den_value_2 = kpi_line[Const.DEN_VALUES_2].split(',')
            general_filters[den_type_2] = den_value_2
        numerator_filters = {num_type: num_value}
        if kpi_line[Const.NUM_TYPES_2]:
            num_type_2 = kpi_line[Const.NUM_TYPES_2]
            num_value_2 = kpi_line[Const.NUM_VALUES_2].split(',')
            numerator_filters[num_type_2] = num_value_2
        numerator_facings = self.scif[self.get_filter_condition(self.scif, **numerator_filters)][
            self.facings_field].sum()
        denominator_facings = self.scif[self.get_filter_condition(self.scif, **general_filters)][
            self.facings_field].sum()
        general_filters['Southwest Deliver'] = 'Y'
        number_of_shelves_value = self.match_product_in_scene[self.get_filter_condition(
            self.match_product_in_scene, **general_filters)]['shelf_number'].unique().count()

        number_of_shelves_score = numerator_facings / float(denominator_facings / float(number_of_shelves_value))

        if target:
            score = 1 if number_of_shelves_score >= target else 0
        else:
            score = 1
            target = 0
        return number_of_shelves_score, score, target

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

        if kpi_type == Const.AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.SHELVES or kpi_type == Const.SHELVES_BONUS:
            return self.calculate_number_of_shelves
        elif kpi_type == Const.SHELVES_BONUS:
            return self.calculate_number_of_shelves_bonus
        elif kpi_type == Const.FACINGS:
            return self.calculate_facings_ntba
        elif kpi_type == Const.RATIO:
            return self.calculate_ratio
        elif kpi_type == Const.PURITY:
            return self.sos_with_num_and_dem
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
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


    def choose_and_write_results(self):
        """
        writes all the KPI in the DB: first the session's ones, second the scene's ones and in the end the ones
        that depends on the previous ones. After all it writes the red score
        """
        # self.scenes_results.to_csv('results/{}/scene {}.csv'.format(self.calculation_type, self.session_uid))####
        # self.session_results.to_csv('results/{}/session {}.csv'.format(self.calculation_type, self.session_uid))####
        main_template = self.templates[Const.KPIS]
        self.write_session_kpis(main_template)
        # self.write_condition_kpis(main_template)
        # self.write_missings(main_template)
        self.write_to_db(SUB_PROJECT, 0)
        # result_dict = {Const.KPI_NAME: 'RED SCORE', Const.SCORE: self.red_score}####
        # self.all_results = self.all_results.append(result_dict, ignore_index=True)####
        # self.all_results.to_csv('results/{}/{}.csv'.format(self.calculation_type, self.session_uid))####

    def write_missings(self, main_template):
        """
        write 0 in all the KPIs that didn't get score
        :param main_template:
        """
        for i, main_line in main_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            if not self.all_results[self.all_results[Const.KPI_NAME] == kpi_name].empty:
                continue
            result = 0
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_session_kpis(self, main_template):
        """
        iterates all the session's KPIs and saves them
        :param main_template: main_sheet.
        """
        # session_template = main_template[main_template[Const.CONDITION] == ""]
        # if self.calculation_type == Const.SOVI:
        #     session_template = session_template[session_template[Const.SESSION_LEVEL] == Const.V]
        for i, main_line in main_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            result = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            if result.empty:
                continue
            result = result.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_regular_scene_kpis(self, scene_template):
        """
        lets the regular KPIs choose their scenes (if they passed).
        Like in the incremental part - if KPI passed some scenes, we will choose the scene that the children passed
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed incremental)
        :return: the new template (without the KPI written already)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.SCENE_FK].isin(self.used_scenes))]
            true_results = kpi_results[kpi_results[Const.RESULT] > 0]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if true_results.empty:
                continue
            true_results = true_results.sort_values(by=Const.RESULT, ascending=False)
            scene_fk = true_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.RESULT], display_text, weight,
                                     scene_fk=scene_fk, reuse_scene=reuse_scene)
            scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
        return scene_template

    def write_not_passed_scene_kpis(self, scene_template):
        """
        lets the KPIs not passed choose their scenes.
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed KPIs)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.SCENE_FK].isin(self.used_scenes))]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if kpi_results.empty:
                continue
            scene_fk = kpi_results.iloc[0][Const.SCENE_FK]
            self.write_to_all_levels(kpi_name, 0, display_text, weight, scene_fk=scene_fk, reuse_scene=reuse_scene)

    def write_scene_kpis(self, main_template):
        """
        iterates every scene_kpi that does not depend on others, and choose the scene they will take:
        1. the incrementals take their scene (if they passed).
        2. the regular KPIs that passed choose their scenes.
        3. the ones that didn't pass choose their random scenes.
        :param main_template: main_sheet.
        """
        scene_template = main_template[(main_template[Const.SESSION_LEVEL] != Const.V) &
                                       (main_template[Const.CONDITION] == "")]
        scene_template = self.write_incremental_kpis(scene_template)
        scene_template = self.write_regular_scene_kpis(scene_template)
        self.write_not_passed_scene_kpis(scene_template)

    def write_condition_kpis(self, main_template):
        """
        writes all the KPI that depend on other KPIs by checking if the parent KPI has passed and in which scene.
        :param main_template: main_sheet
        """
        condition_template = main_template[main_template[Const.CONDITION] != '']
        for i, main_line in condition_template.iterrows():
            condition = main_line[Const.CONDITION]
            kpi_name = main_line[Const.KPI_NAME]
            if self.calculation_type == Const.MANUAL or main_line[Const.SESSION_LEVEL] == Const.V:
                kpi_results = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            else:
                kpi_results = self.scenes_results[self.scenes_results[Const.KPI_NAME] == kpi_name]
            condition_result = self.all_results[(self.all_results[Const.KPI_NAME] == condition) &
                                                (self.all_results[Const.RESULT] > 0)]
            if condition_result.empty:
                continue
            condition_result = condition_result.iloc[0]
            condition_scene = condition_result[Const.SCENE_FK]
            if condition_scene and Const.SCENE_FK in kpi_results:
                results = kpi_results[kpi_results[Const.SCENE_FK] == condition_scene]
            else:
                results = kpi_results
            if results.empty:
                continue
            result = results.iloc[0][Const.RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            scene_fk = results.iloc[0][Const.SCENE_FK] if Const.SCENE_FK in kpi_results else None
            self.write_to_all_levels(kpi_name, result, display_text, weight, scene_fk=scene_fk)

    def get_sw_scenes(self):
        return self.scif[self.scif['Southwest Deliver'] == 'Y']['scene_id'].unique().tolist()

    def get_weight_factor(self):
        sum_weights = self.templates[Const.KPIS][Const.WEIGHT].sum()
        return sum_weights / 100.0

    def get_score(self, weight):
        return weight / self.weight_factor

    def update_parents(self, kpi_name, result, score):
        parent = self.get_kpi_parent(kpi_name)
        if parent != SUB_PROJECT:
            if 'Bonus' in parent:
                self.update_sub_score(kpi_name, passed=result)
            else:
                self.update_sub_score(kpi_name, passed=score)

    def get_kpi_parent(self, kpi_name):
        type_name = '{} {}'.format(SUB_PROJECT, kpi_name)
        kpi_family_fk = int(self.common_db2.kpi_static_data.set_index('type')\
                                .loc[type_name, 'kpi_family_fk'])
        if kpi_family_fk in Const.KPI_FAMILY_KEY:
            return Const.KPI_FAMILY_KEY[kpi_family_fk]
        else:
            return SUB_PROJECT

    def update_sub_score(self, kpi_name, passed=0, parent=None):
        if not parent:
            parent = self.get_kpi_parent(kpi_name)
        if parent == SUB_PROJECT:
            parent = '{} {}'.format(SUB_PROJECT, kpi_name)
        if 'Bonus' not in kpi_name:
            self.sub_totals[parent] += 1
            if passed:
                self.sub_scores[parent] += passed
        else:
            self.sub_totals[parent] += 0
            self.sub_scores[parent] += 0

    def write_to_db(self, kpi_name, score, result=None, threshold=None, num=None, den=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type('{} {}'.format(SUB_PROJECT, kpi_name))
        parent = self.get_kpi_parent(kpi_name)
        delta = 0
        if isinstance(threshold, str) and '%' in threshold:
            if score == 0:
                targ = float(threshold.split('-')[0].replace('%', ''))/100
                delta = round((targ * den) - num)
            threshold = self.tools.result_values[threshold.replace(' ', '')]

        if parent != SUB_PROJECT:
            if score == 1:
                score = Const.PASS
            elif score == 0:
                score = Const.FAIL
            else:
                score = 'bonus'
            score = self.tools.result_values[score]
        self.common_db2.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=threshold,
                                           numerator_result=num, denominator_result=den, weight=delta,
                                           identifier_parent=self.common_db2.get_dictionary(parent_name=parent),
                                           numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id)
        # self.write_to_db_result(
        #     self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 2), score=score, level=2)
        # self.write_to_db_result(
        #     self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 3), score=score, level=3,
        #     threshold=threshold, result=result)

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

    def kpi_parent_result(self, parent, num, den):
        if parent in Const.PARENT_NOT_RATIO:
            result = num
        else:
            if den:
                result = round((float(num) / den)*100, 2)
            else:
                result = 0

        return result

    def write_sub_parents(self):
        for sub_parent in self.sub_totals.keys():
        # for sub_parent in set(Const.KPI_FAMILY_KEY.values()):
            kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(sub_parent)
            num = self.sub_scores[sub_parent]
            den = self.sub_totals[sub_parent]
            result = self.kpi_parent_result(sub_parent, num, den)
            if 'Bonus' in sub_parent:
                den = 0
            self.common_db2.write_to_db_result(fk=kpi_fk, numerator_result=num, numerator_id=self.manufacturer_fk,
                                               denominator_id=self.store_id,
                                               denominator_result=den, result=result, score=result, target=100,
                                               identifier_result=self.common_db2.get_dictionary(
                                                   parent_name=sub_parent),
                                               identifier_parent=self.common_db2.get_dictionary(
                                                   parent_name=Const.PARENT_HIERARCHY[sub_parent]),
                                               should_enter=True)

    def write_parent(self):
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(SUB_PROJECT)
        num = sum([self.sub_scores[key] for key, value in Const.PARENT_HIERARCHY.items() if value == Const.CMA])
        den = sum([self.sub_totals[key] for key, value in Const.PARENT_HIERARCHY.items() if value == Const.CMA])
        if den:
            result = num * 100.0 / den
            self.common_db2.write_to_db_result(fk=kpi_fk, numerator_result=num, numerator_id=self.manufacturer_fk,
                                               denominator_id=self.store_id,
                                               denominator_result=den, result=result, score=result, target=100,
                                               identifier_result=self.common_db2.get_dictionary(
                                                   parent_name=SUB_PROJECT))

    def commit_results(self):
        """
        committing the results in both sets
        """
        pass
        # self.common_db.delete_results_data_by_kpi_set()
        # self.common_db.commit_results_data_without_delete()
        self.common_db2.commit_results_data()
        # if self.common_db_integ:
        #     self.common_db_integ.delete_results_data_by_kpi_set()
        #     self.common_db_integ.commit_results_data_without_delete()
