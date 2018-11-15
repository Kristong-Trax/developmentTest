from datetime import datetime
import os
import pandas as pd
from collections import defaultdict

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Utils import Validation
from Trax.Utils.Logging.Logger import Log
from Projects.GMIUS.Utils.Const import Const
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Projects.GMIUS.Utils.PositionGraph import Block
from KPIUtils_v2.Calculations.BlockCalculations import Block as Block2
from Projects.GMIUS.Utils.BlockCalculations import Block as Block2

# from KPIUtils_v2.Calculations.BlockCalculations import Block


__author__ = 'Sam'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data', 'GMI KPI Template v0.2.xlsx')


class ToolBox:

    def __init__(self, data_provider, output, common):
        self.common = common
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider.all_templates
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.result_values_dict = self.make_result_values_dict()
        # self.store_assortment = self.ps_data_provider.get_store_assortment()
        # self.store_sos_policies = self.ps_data_provider.get_store_policies()
        # self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        # self.labels = self.ps_data_provider.get_labels()
        # self.scene_results = self.ps_data_provider.get_scene_results(self.scenes)



        ''' stop gap until the real data arrives'''

        self.data_provider2 = KEngineDataProvider('rinielsenus')
        # self.data_provider2.load_session_data('c5ef379f-54d6-45b3-b907-303a81fc1876')
        self.data_provider2.load_session_data('ff204427-bf76-4af6-8284-462edd4c75ab')
        self.scene_info = self.data_provider2[Data.SCENES_INFO]
        self.all_products = self.data_provider2[Data.ALL_PRODUCTS]
        self.products = self.data_provider2[Data.PRODUCTS]
        self.store_info = self.data_provider2[Data.STORE_INFO]
        self.store_id = self.data_provider2[Data.STORE_FK]
        self.match_product_in_scene = self.data_provider2[Data.MATCHES]
        self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk')\
                                               .merge(self.scene_info, on='scene_fk')\
                                               .merge(self.templates, on='template_fk')
        self.visit_date = self.data_provider2[Data.VISIT_DATE]
        self.session_info = self.data_provider2[Data.SESSION_INFO]
        self.scenes = self.scene_info['scene_fk'].tolist()
        self.scif = self.data_provider2[Data.SCENE_ITEM_FACTS]

        self.template = {}
        for sheet in Const.SHEETS:
            self.template[sheet] = pd.read_excel(TEMPLATE_PATH, sheet)
        self.hierarchy = self.template[Const.KPIS].set_index(Const.KPI_NAME)[Const.PARENT].to_dict()
        self.template[Const.KPIS] = self.template[Const.KPIS][(self.template[Const.KPIS][Const.TYPE] != Const.PARENT) &
                                                              (~self.template[Const.KPIS][Const.TYPE].isnull())]
        self.sub_scores = defaultdict(int)
        self.sub_totals = defaultdict(int)

    # main functions:
    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.template[Const.KPIS]
        for i, main_line in main_template.iterrows():
            self.calculate_main_kpi(main_line)
            # self.graph(None, None)

    def calculate_main_kpi(self, main_line):
        kpi_name = main_line[Const.KPI_NAME]
        kpi_type = main_line[Const.TYPE]
        scene_types = self.read_cell_from_line(main_line, Const.SCENE_TYPE)
        print(kpi_name, kpi_type)
        general_filters = {}
        relevant_scif = self.scif.copy()
        # if scene_types:
        #     relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        #     general_filters['template_name'] = scene_types
        # if relevant_scif.empty:
        #     return
        function = self.get_kpi_function(kpi_type)
        if kpi_type == Const.TMB:
            for i, kpi_line in self.template[kpi_type].iterrows():
                function(kpi_name, kpi_line, relevant_scif, general_filters)
                # result, num, den, score, target = function(kpi_line)
                # if (result is None and score is None and target is None) or not den:
                #     continue
                # self.update_parents(kpi_name, score)
                # self.write_to_db(kpi_name, kpi_type, score, result=result, threshold=target, num=num, den=den)

    def calculate_sos(self, kpi_name, kpi_line, relevant_scif, general_filters):
        levels = self.read_cell_from_line(kpi_line, Const.AGGREGATION_LEVELS)
        sos_types = self.read_cell_from_line(kpi_line, Const.SOS_TYPE)
        scif = self.filter_df(self.scif.copy(), Const.SOS_EXCLUDE_FILTERS, exclude=1)
        scif['count'] = Const.MM_TO_FT
        for level in levels:
            level_col = '{}_fk'.format(level).lower()
            groups = scif.groupby(level_col)
            for item_pk, df in groups:
                for sos_type in sos_types:
                    if sos_type in Const.SOS_COLUMN_DICT:
                        sos_sum_col = Const.SOS_COLUMN_DICT[sos_type]
                    else:
                        Log.warning("SOS Type not found in Const.SOS_COLUMN_DICT in kpi {}".format(kpi_name))
                        return
                    den = scif[sos_sum_col].sum()
                    kpi_fk = self.common.get_kpi_fk_by_kpi_type('{} {} {}'.format(level, kpi_name, sos_type))
                    num = df[sos_sum_col].sum() / Const.MM_TO_FT
                    ratio, score = self.ratio_score(num, den)
                    self.common.write_to_db_result(fk=kpi_fk, score=score, result=ratio, numerator_id=item_pk,
                                                   numerator_result=num, denominator_result=den,
                                                   denominator_id=self.store_id)

    def calculate_topmiddlebottom(self, kpi_name, kpi_line, relevant_scif, general_filters):
        locations = set()
        map = self.template[Const.TMB_MAP].set_index('Num Shelves').to_dict('index')
        filters = self.get_kpi_line_filters(kpi_line)
        filters.update(general_filters)

        bay_max_shelf = self.filter_df(self.mpis, general_filters).set_index('bay_number')\
                                                                  .groupby(level=0)['shelf_number'].max().to_dict()
        mpis = self.filter_df(self.mpis, filters)
        if mpis.empty:
            return
        grouped_mpis = mpis.set_index('bay_number').groupby(level=0)

        for bay, shelves in grouped_mpis:
            sub_map = map[bay_max_shelf[bay]]
            shelf_with_most = shelves.groupby('shelf_number')[shelves.columns[0]].count().sort_values().index[-1]
            locations.add(sub_map[shelf_with_most])
            # for shelf in shelves:
            #     locations.add(sub_map[shelf])
            if len(locations) == 3:
                break

        locations = sorted(list(locations))[::-1]
        ordered_result = '-'.join(locations)


    def load_tmb_map(self):
        pass

    def graph(self, kpi_name, kpi_line, relevant_scif, general_filters):
        # g = Block(self.data_provider2)
        g2 = Block2(self.data_provider2)
        prod = self.all_products.drop(['width_mm', 'height_mm'], axis=1)
        mpis = self.mpis.copy()
        mpis = self.pos_scrubber(mpis)
        relevant_filter = {'Segment': 'DRY DOG NATURAL/GRAIN FREE'}
        allowed_filter = {'product_type': ['Empty', 'Other']}
        filtered_mpis = mpis[(mpis['Segment'] == 'DRY DOG NATURAL/GRAIN FREE') |
                       (mpis['product_type'].isin(['Empty', 'Other']))]
        # g.alt_block(filtered_mpis, mpis, relevant_filter, allowed_filter)
        print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n')
        # relevant_filter.update(allowed_filter)
        g, c = g2.network_x_block_together2(relevant_filter, additional={'allowed_products_filters': allowed_filter,
                                                                     'include_stacking': False})

    @staticmethod
    def filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    @staticmethod
    def ratio_score(num, den, target=None):
        ratio = 0
        if den:
            ratio = round(num*100.0/den, 2)
        score = 1 if ratio >= target and target else 0
        return ratio, score

    @staticmethod
    def read_cell_from_line(line, col):
        try:
            val = line[col]
        except:
            val = None
        if val:
            if hasattr(val, 'split'):
                if ', ' in val:
                    val = val.split(', ')
                elif ',' in val:
                    val = val.split(',')
            if not isinstance(val, list):
                val = [val]
        # if pd.isnull(val[0]):
        #     val = []
        return val

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
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(ret, 'split'):
            ret = ret.split(delimiter)
        return ret

    @staticmethod
    def sos_with_num_and_dem(kpi_line, num_scif, den_scif, facings_field):

        try:
            Validation.is_empty_df(den_scif)
            Validation.df_columns_equality(den_scif, num_scif)
            Validation.is_subset(den_scif, num_scif)
        except Exception, e:
            msg = "Data verification failed: {}.".format(e)
            return None, None, None

        den = den_scif[facings_field].sum()
        try:
            Validation.is_empty_df(num_scif)
        except Exception as e:
            return (0, 0, den)

        num = num_scif[facings_field].sum()
        if den:
            ratio = round((num / float(den))*100, 2)
        else:
            ratio = 0

        return ratio, num, den

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.AGGREGATION:
            return self.calculate_sos
        elif kpi_type == Const.TMB:
            return self.calculate_topmiddlebottom
        else:
            Log.warning("The value '{}' in column sheet in the template is not recognized".format(kpi_type))
            return None

    def make_result_values_dict(self):
        query = "SELECT * FROM static.kpi_result_value;"
        return pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).set_index('value')['pk'].to_dict()


    def write_to_db(self, kpi_name, kpi_type, score, result=None, threshold=None, num=None, den=None):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float
        :param display_text: str
        :param result: str
        :param threshold: int
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(self.get_kpi_name(kpi_name, kpi_type))
        parent = self.get_parent(kpi_name)
        self.common.write_to_db_result(fk=kpi_fk, score=score, result=result, should_enter=True, target=threshold,
                                           numerator_result=num, denominator_result=den,
                                           identifier_parent=self.common.get_dictionary(parent_name=parent))

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


    # def kpi_parent_result(self, parent, num, den):
    #     if parent in Const.PARENT_RATIO:
    #         if den:
    #             result = round((float(num) / den)*100, 2)
    #         else:
    #             result = 0
    #     else:
    #         result = num
    #     return result
    #
    # def write_family_tree(self):
    #     for sub_parent in self.sub_totals.keys():
    #         # for sub_parent in set(Const.KPI_FAMILY_KEY.values()):
    #         kpi_type = sub_parent
    #         if sub_parent != SUB_PROJECT:
    #             kpi_type = '{} {}'.format(SUB_PROJECT, sub_parent)
    #         kpi_fk = self.common_db2.get_kpi_fk_by_kpi_type(kpi_type)
    #         num = self.sub_scores[sub_parent]
    #         den = self.sub_totals[sub_parent]
    #         result, score = self.ratio_score(num, den, 1)
    #         self.common_db2.write_to_db_result(fk=kpi_fk, numerator_result=num, numerator_id=Const.MANUFACTURER_FK,
    #                                            denominator_id=self.store_id,
    #                                            denominator_result=den, result=result, score=num, target=den,
    #                                            identifier_result=self.common_db2.get_dictionary(
    #                                                parent_name=sub_parent),
    #                                            identifier_parent=self.common_db2.get_dictionary(
    #                                                parent_name=self.get_parent(sub_parent)),
    #                                            should_enter=True)

    def pos_scrubber(self, matches):
        columns_of_import = ['scene_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']
        pos_mask = matches['product_type'] == 'POS'
        pos = matches[pos_mask]
        for i, row in pos.iterrows():
            row = matches.loc[i]
            if row[columns_of_import].iloc[1:].sum() != -3:  # We can ignore POS that was correctly not given shelfspace
                base_mask = ((matches['scene_fk'] == row['scene_fk']) &
                             (matches['bay_number'] == row['bay_number']) &
                             (matches['shelf_number'] == row['shelf_number']))
                type_mask = (base_mask & (matches['facing_sequence_number'] == row['facing_sequence_number']))
                stacking_mask = (type_mask & (matches['stacking_layer'] >= row['stacking_layer']))
                matches.loc[stacking_mask, 'stacking_layer'] -= 1

                if len(set(matches[type_mask]['product_type'])) <= 1:
                    sequence_mask = (base_mask & (matches['facing_sequence_number'] > row['facing_sequence_number']))
                    matches.loc[sequence_mask, 'facing_sequence_number'] -= 1
                    matches.loc[base_mask, 'n_shelf_items'] -= 1

        matches = matches[matches['product_type'] != 'POS']
        matches.loc[matches['stacking_layer'] == 1, 'status'] = 1
        return matches