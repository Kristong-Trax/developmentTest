import os
from datetime import datetime
import pandas as pd
import numpy as np
from collections import defaultdict
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.ARA.Const import Const
from Projects.CCBOTTLERSUS_SAND.Utils.SOS import Shared
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS




__author__ = 'Uri, Sam'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', Const.TEMPLATE_PATH)
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
SUB_PROJECT = 'ARA'


class ARAToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, common_db2):
        self.output = output
        self.data_provider = data_provider
        self.common_db = Common(self.data_provider, SUB_PROJECT)
        self.common_db2 = common_db2
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
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.sw_scenes = self.get_relevant_scenes() # we don't need to check scenes without United products
        self.survey = Survey(self.data_provider, self.output)
        self.sos = SOS(self.data_provider, self.output)
        self.results = self.data_provider[Data.SCENE_KPI_RESULTS]
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        self.sales_center = self.store_info['additional_attribute_5'].iloc[0]
        if self.store_type in STORE_TYPES: #####
            self.store_type = STORE_TYPES[self.store_type] ####
        self.store_attr = self.store_info['additional_attribute_3'].iloc[0]
        # self.kpi_static_data = self.common_db.get_kpi_static_data()
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.sub_scores = defaultdict(int)
        self.sub_totals = defaultdict(int)
        self.templates = self.get_template()
        self.hierarchy = self.templates[Const.KPIS].set_index(Const.KPI_NAME)[Const.PARENT].to_dict()
        self.templates = self.get_relevant_template(self.templates)
        self.children = self.templates[Const.KPIS][Const.KPI_NAME]
        self.tools = Shared(self.data_provider, self.output)

    # main functions:
    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
        self.write_family_tree()
        # self.write_to_db_result(

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        # if kpi_name not in Const.ALL_SCENE_KPIS:  # placeholder- need to check for unintended consequences
        #     relevant_scif = self.scif[self.scif['scene_id'].isin(self.sw_scenes)]
        # else:
        #     relevant_scif = self.scif.copy()
        relevant_scif = self.scif.copy()
        result = score = target = None
        general_filters = {}

        scene_types = self.does_exist(main_line, Const.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
            general_filters['template_name'] = scene_types
        scene_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
            general_filters['template_group'] = scene_groups

        relevant_scif = relevant_scif[relevant_scif['product_type'] != "Empty"]
        relevant_template = self.templates[kpi_type]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        function = self.get_kpi_function(kpi_type)

        if not relevant_scif.empty:
            for i, kpi_line in relevant_template.iterrows():
                result, num, den, score, target = function(kpi_line, relevant_scif, general_filters)
                if (result is None and score is None and target is None) or not den:
                    continue
                self.update_parents(kpi_name, score)
                self.write_to_db(kpi_name, kpi_type, score, result=result, threshold=target, num=num, den=den)


    def get_template(self):
        template = {}
        for sheet in Const.SHEETS:
            template[sheet] = pd.read_excel(TEMPLATE_PATH, sheetname=sheet).fillna('')
        return template

    def get_relevant_template(self, template):
        kpis = template[Const.KPIS]
        template[Const.KPIS] = kpis[(self.is_or_none(kpis, Const.REGION, self.region)) &
                                    (self.is_or_none(kpis, Const.STORE_TYPE, self.store_type)) &
                                    (self.is_or_none(kpis, Const.PROGRAM, self.store_attr)) &
                                    (kpis[Const.SESSION_LEVEL] == 'Y') &
                                    (kpis[Const.TYPE] != Const.PARENT)]
        return template

    def is_or_none(self, template, col, val):
        if not isinstance(val, list):
            val = [val]
        return ((template[col].isin(val)) |
                (template[col] is None) |
                (template[col] == ''))

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
        general_filters['product_type'] = (['Empty', 'Irrelevant'], 0)
        relevant_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        target = self.get_targets(kpi_name)

        sos_filters = self.get_kpi_line_filters(kpi_line, name='numerator')
        general_filters = self.get_kpi_line_filters(kpi_line, name='denominator')
        exclude_filters = {key: (val, self.EXCLUDE_FILTER) for key, val in
                           self.get_kpi_line_filters(kpi_line, name='exclude').items()}

        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **sos_filters)]
        den_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        if exclude_filters:
            num_scif = num_scif[self.get_filter_condition(num_scif, **exclude_filters)]
        sos_value, num, den = self.tools.sos_with_num_and_dem(kpi_line, num_scif, den_scif, self.facings_field)

        target *= 100
        score = 1 if sos_value >= target else 0
        target = '{}%'.format(int(target))

        return sos_value, num, den, score, target

    def calculate_min_facings(self, kpi_line, relevant_scif, general_filters):
        num_scif, den_scif, target = self.calculation_base(kpi_line, relevant_scif, general_filters)
        num = num_scif[self.facings_field].sum()
        score = 1 if num >= target else 0

        return None, num, None, score, target

    def calculate_min_skus(self, kpi_line, relevant_scif, general_filters):
        num_scif, den_scif, target = self.calculation_base(kpi_line, relevant_scif, general_filters)
        location = self.does_exist(kpi_line, Const.LOCATION)
        num = num_scif.shape[0]
        score = 1 if num >= target else 0

        return None, num, None, score, target

    def calculate_ratio(self, kpi_line, relevant_scif, general_filters):
        min_facings_percent = kpi_line[Const.MIN_FACINGS]
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
            if sos_value >= min_facings_percent:
                us += 1
            else:
                them += 1

        target = self.get_targets(kpi_line[Const.KPI_NAME]) * 100
        ratio, score = self.ratio_score(us, them, target)
        target = '{}%'.format(int(target))

        return ratio, us, them, score, target

    def calculate_location(self, kpi_line, relevant_scif, general_filters):
        location = self.does_exist(kpi_line, Const.SHELVES)
        mpis = self.match_product_in_scene.merge(self.all_products, on='product_fk')
        mpis = mpis.merge(self.scene_info, on='scene_fk')
        mpis = mpis.merge(self.data_provider[Data.TEMPLATES], on='template_fk')

        num_mpis, den_mpis, target = self.calculation_base(kpi_line, mpis, general_filters)
        den_mpis = num_mpis.copy()
        num_mpis = num_mpis[num_mpis['shelf_number'].isin(location)]
        num = num_mpis.shape[0]
        den = den_mpis.shape[0]
        target *= 100
        ratio, score = self.ratio_score(num, den, target)
        target = '{}%'.format(int(target))
        return ratio, num, den, score, target

    def calculate_min_shelves(self, kpi_line, relevant_scif, general_filters):
        """
        calculates SOS line in the relevant scif.
        :param kpi_line: line from SOS sheet.
        :param relevant_scif: filtered scif.
        :param isnt_dp: if "store attribute" in the main sheet has DP, and the store is not DP, we should filter
        all the DP products out of the numerator.
        :return: boolean
        """
        num_scif, den_scif, target = self.calculation_base(kpi_line, relevant_scif, general_filters)
        num = num_scif[self.facings_field].sum()
        den = den_scif[self.facings_field].sum()
        relevant_scenes = relevant_scif['scene_fk'].unique().tolist()
        scene_filters = {'scene_fk': relevant_scenes}
        num_shelves = self.match_product_in_scene[self.get_filter_condition(
                                                  self.match_product_in_scene, **scene_filters)]\
                                                  [['scene_fk', 'bay_number', 'shelf_number']]\
                                                  .drop_duplicates().shape[0]
        ratio, score = self.ratio_score(num, float(den)/num_shelves, target)
        return ratio, num, None, score, target

    def calculation_base(self, kpi_line, relevant_scif, general_filters):
        kpi_name = kpi_line[Const.KPI_NAME]
        numerator_filters = self.get_kpi_line_filters(kpi_line)
        target = self.get_targets(kpi_name)

        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        num_scif = relevant_scif[self.get_filter_condition(relevant_scif, **numerator_filters)]
        den_scif = relevant_scif[self.get_filter_condition(relevant_scif, **general_filters)]
        return num_scif, den_scif, target

    # helpers:
    def get_targets(self, kpi_name):
        targets_template = self.templates[Const.TARGETS]
        store_targets = targets_template.loc[(self.is_or_none(targets_template, Const.PROGRAM, self.program))]
        filtered_targets_to_kpi = store_targets.loc[targets_template[Const.KPI_NAME] == kpi_name]
        if not filtered_targets_to_kpi.empty:
            target = filtered_targets_to_kpi[Const.TARGET].values[0]
        else:
            target = None
        return target

    @staticmethod
    def ratio_score(num, den, target):
        ratio = 0
        if den:
            ratio = round(num*100.0/den, 2)
        score = 1 if ratio >= target else 0
        return ratio, score

    def get_kpi_line_filters(self, kpi_orig, name=''):
        kpi_line = kpi_orig.copy()
        if name:
            name = name.lower() + ' '
        filters = defaultdict(list)
        attribs = [x.lower() for x in kpi_line.index]
        kpi_line.index = attribs
        c = 1
        while 1:
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)]:
                filters[kpi_line['{}param {}'.format(name, c)]] += self.splitter(kpi_line['{}value {}'.format(name, c)])
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
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(ret, 'split'):
            ret = ret.split(delimiter)
        return ret

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

        if kpi_type == Const.SOS:
            return self.calculate_sos
        elif kpi_type == Const.MIN_SHELVES:
            return self.calculate_min_shelves
        elif kpi_type == Const.MIN_FACINGS:
            return self.calculate_min_facings
        elif kpi_type == Const.LOCATION:
            return self.calculate_location
        elif kpi_type == Const.MIN_SKUS:
            return self.calculate_min_skus
        elif kpi_type == Const.RATIO:
            return self.calculate_ratio
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

    def get_relevant_scenes(self):
        return self.scif[self.scif[Const.DELIVER] == 'Y']['scene_id'].unique().tolist()

    def get_kpi_name(self, kpi_name, kpi_type):
        return '{} {} {}'.format(SUB_PROJECT, kpi_name, kpi_type)

    def get_parent(self, kpi_name):
        try:
            parent = self.hierarchy[kpi_name]
        except Exception as e:
            parent = None
            Log.warning("Warning, Parent KPI not found in column '{}' on template page '{}'"
                        .format(Const.KPI_NAME, Const.KPIS))
        return parent

    def update_parents(self, kpi, score):
        parent = self.get_parent(kpi)
        while parent:
            self.update_sub_score(parent, score=score)
            parent = self.get_parent(parent)

    def update_sub_score(self, parent, score=0):
        self.sub_totals[parent] += 1
        self.sub_scores[parent] += score

    def write_to_db(self, kpi_name, kpi_type, score, result=None, threshold=None, num=None, den=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(self.get_kpi_name(kpi_name, kpi_type))
        parent = self.get_parent(kpi_name)
        delta = 0
        if isinstance(threshold, str) and '%' in threshold:
            threshold = float(threshold.split('-')[0].replace('%', ''))
            if score == 0:
                targ = threshold/100
                delta = round((targ * den) - num)
        else:
            delta = threshold - num

        if kpi_name in self.children:
            if score == 1:
                score = Const.PASS
            elif score == 0:
                score = Const.FAIL
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
        if parent in Const.PARENT_RATIO:
            if den:
                result = round((float(num) / den)*100, 2)
            else:
                result = 0
        else:
            result = num
        return result

    def write_family_tree(self):
        for sub_parent in self.sub_totals.keys():
            # for sub_parent in set(Const.KPI_FAMILY_KEY.values()):
            kpi_type = sub_parent
            if sub_parent != SUB_PROJECT:
                kpi_type = '{} {}'.format(SUB_PROJECT, sub_parent)
            kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(kpi_type)
            num = self.sub_scores[sub_parent]
            den = self.sub_totals[sub_parent]
            result, score = self.ratio_score(num, den, 1)
            self.common_db2.write_to_db_result(fk=kpi_fk, numerator_result=num, numerator_id=Const.MANUFACTURER_FK,
                                               denominator_id=self.store_id,
                                               denominator_result=den, result=result, score=num, target=den,
                                               identifier_result=self.common_db2.get_dictionary(
                                                   parent_name=sub_parent),
                                               identifier_parent=self.common_db2.get_dictionary(
                                                   parent_name=self.get_parent(sub_parent)),
                                               should_enter=True)

    def commit_results(self):
        """
        committing the results in both sets
        """
        pass
        # self.common_db.delete_results_data_by_kpi_set()
        # self.common_db.commit_results_data_without_delete()
        # self.common_db2.commit_results_data()
        # if self.common_db_integ:
        #     self.common_db_integ.delete_results_data_by_kpi_set()
        #     self.common_db_integ.commit_results_data_without_delete()
