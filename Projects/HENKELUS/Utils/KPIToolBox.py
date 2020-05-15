from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
import numpy as np
from Projects.HENKELUS.Utils.HenkelDataProvider import HenkelDataProvider
from Projects.HENKELUS.Data.LocalConsts import Consts
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AdjacencyCalculations_v2 import Adjancency

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from collections import Counter

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph


# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'


class ToolBox(GlobalSessionToolBox):
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.kpi_template_path = Consts.KPI_TEMPLATE_PATH
        self.kpi_template = self.parse_template(self.kpi_template_path, Consts.relevant_kpi_sheets)
        self.manufacturer_fk = Consts.OWN_MANUFACTURER_FK
        self.hdp = HenkelDataProvider(self.data_provider)
        self.block = Block(self.data_provider)
        self.adjacency = Adjancency(self.data_provider)
        self.toolbox = GENERALToolBox(self.data_provider)
        self.mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

    def main_calculation(self):
        self.calculate_max_block_directional()
        self.calculate_sku_count()
        self.calculate_facing_count()
        self.calculate_smart_tags()
        self.calculate_base_measurement()
        self.calculate_liner_measure()
        self.calculate_horizontal_shelf_position()
        self.calculate_vertical_shelf_position()
        self.calculate_blocking()
        self.calculate_blocking_comp()

        # self.calculate_blocking_Sequence()
        self.calculate_max_blocking_adj()
        self.calculate_negative_max_blocking_adj()
        # self.calculate_blocking_orientation()

        score = 0
        return score

    def calculate_max_block_directional(self):
        template = self.kpi_template[Consts.MAX_BLOCK_DIRECTIONAL_ADJACENCY_SHEET]
        for i, row in template.iterrows():
            kpi_name,kpi_fk = self._get_kpi_name_and_fk(row)

            # dataset a: main block products
            relevant_product_fks_with_parent_brand_all_and_non_sensitive = \
                self._get_product_fks_with_filter(self.scif,{ 'Parent Brand': ['ALL'],'Sensitive': ['NOT SENSITIVE SKIN'],
                                                                'product_type': ['SKU']})
            # dataset b: adjacent block products
            relevant_product_fks_with_parent_brand_all_and_sensitive = \
                self._get_product_fks_with_filter(self.scif,{ 'Parent Brand': ['ALL'],'Sensitive': ['SENSITIVE SKIN'],
                                                                'product_type': ['SKU']})

            result = 0
            if relevant_product_fks_with_parent_brand_all_and_non_sensitive.size != 0 and relevant_product_fks_with_parent_brand_all_and_sensitive.size != 0:
                relevant_filters_for_blocka = {'product_fk': relevant_product_fks_with_parent_brand_all_and_non_sensitive} #products of dataset a
                additional_filter_blocka = {'allowed_edge_type': ['connected'],
                  'use_masking_only': True, 'calculate_all_scenes': True, 'allow_products_filters': {
                'product_type': ['Empty', 'Other'],'minimum_facing_for_block': 1, 'Smart Tag': 'additional display'}}

                relevant_filters_for_blockb = {'product_fk': relevant_product_fks_with_parent_brand_all_and_sensitive} # products of dataset b
                additional_filter_blockb = {'allowed_edge_type': ['connected'],'use_masking_only': True,
                'calculate_all_scenes': True, 'allow_products_filters': {
                      'product_type': ['Empty','Other'], 'minimum_facing_for_block': 1, 'Smart Tag': 'additional display'}}

                dataseta_block = self._get_block(relevant_filters_for_blocka, additional_filter_blocka)
                datasetb_block = self._get_block(relevant_filters_for_blockb, additional_filter_blockb)

                if (not dataseta_block.empty) and (not datasetb_block.empty):
                    if any(np.in1d(dataseta_block.scene_fk, datasetb_block.scene_fk)):
                        datasetb_block = datasetb_block.sort_values('total_facings')
                        result = self._get_adjacent_node_direction(dataseta_block, datasetb_block)

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=1,
                             denominator_id=self.store_id, denominator_result=1, result=result)

    def calculate_vertical_shelf_position(self):
        template = self.kpi_template[Consts.VERTICAL_SHELF_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']
            sub_category_value = row['sub_category']
            shelf_map_df = self.kpi_template[Consts.SHELF_MAP_SHEET]

            mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')

            if not pd.isna(sub_category_value):
                mpis = mpis[mpis['sub_category'] == sub_category_value]

            for value1 in mpis[Params1].unique().tolist():
                filtered_mpis = mpis[mpis[Params1] == value1]

                if not pd.isna(Params2):
                    for value2 in mpis[Params2].unique().tolist():
                        filtered_mpis = filtered_mpis[filtered_mpis[Params2] == value2]
                        self.calculate_position(kpi_fk=kpi_fk, mpis=mpis, filtered_mpis=filtered_mpis,
                                                shelf_map_df=shelf_map_df,
                                                param1=Params1)

                else:
                    self.calculate_position(kpi_fk=kpi_fk, mpis=mpis, filtered_mpis=filtered_mpis,
                                            shelf_map_df=shelf_map_df, param1=Params1)

    def calculate_position(self, kpi_fk=None, mpis=None, filtered_mpis=None, shelf_map_df=None, param1=None):
        shelf_positions = []
        try:
            product_fk = filtered_mpis.product_fk.iloc[0]
        except:
            product_fk = -1

        if param1 == 'product_fk':
            denom_id = self.store_id
        else:
            denom_id = self.manufacturer_fk

        for scene in list(filtered_mpis.scene_fk.unique()):
            filtered_mpis = filtered_mpis[filtered_mpis['scene_fk'] == scene]
            shelf_count = len(self.matches['shelf_number'][self.matches['scene_fk'] == scene].unique())
            for i, row in filtered_mpis.iterrows():
                shelf_number = row['shelf_number']
                pos = shelf_map_df[shelf_number][shelf_map_df['Num Shelves'] == shelf_count]
                shelf_positions.append(pos.iloc[0])

        if len(shelf_positions) == 0:
            pass
        else:

            mode = max(shelf_positions, key=shelf_positions.count)
            if not pd.isna(mode):
                result = Consts.CUSTOM_RESULTS[mode]
                self.write_to_db(fk=kpi_fk,
                                 numerator_id=product_fk,
                                 numerator_result=1,
                                 context_id=product_fk,
                                 denominator_id=denom_id, result=result)

    def calculate_horizontal_shelf_position(self):
        template = self.kpi_template[Consts.HORIZONTAL_SHELF_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            mpis = self.mpis

            for value1 in mpis[Params1].unique().tolist():
                shelf_positions = []
                filtered_mpis = mpis[mpis[Params1] == value1]
                product_fk = filtered_mpis.product_fk.iloc[0]

                for scene in list(filtered_mpis.scene_fk.unique()):
                    filtered_mpis = mpis[mpis['scene_fk'] == scene]
                    bay_count = len(filtered_mpis.bay_number.unique())

                    if bay_count == 1:
                        pos = 'Center'
                    else:
                        factor_bay = round(bay_count / float(3))
                        for i, row in filtered_mpis.iterrows():
                            sku_bay_number = row.bay_number

                            if sku_bay_number <= factor_bay:
                                pos = 'Left'
                            elif sku_bay_number > (bay_count - factor_bay):
                                pos = 'Right'

                    shelf_positions.append(pos)

                mode = max(shelf_positions, key=shelf_positions.count)
                if not pd.isna(mode):
                    result = Consts.CUSTOM_RESULTS[mode]
                    self.write_to_db(fk=kpi_fk,
                                     numerator_id=product_fk,
                                     numerator_result=1,
                                     denominator_id=self.store_id, result=result)

    def calculate_blocking_orientation(self):
        template = self.kpi_template[Consts.BASE_MEASURE_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']

        pass

    def calculate_blocking_Sequence(self):
        template = self.kpi_template[Consts.BLOCKING_SEQUENCE]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            custom_text = 'No'
            result = 0
            block_sequence_filter_dict = {}
            block_adj_mpis = {}
            sequence_letters = ['A','B','C']
            for letter in sequence_letters:
                param_dict = {}
                for param in Consts.BLOCKING_SEQUENCE_DATA_COLUMNS:
                    data_param = row[param.format(letter, 'PARAM')]
                    data_value = self.sanitize_row(row[param.format(letter, 'VALUE')])

                    if not pd.isna(data_param):
                        param_dict[data_param] = data_value

                block_sequence_filter_dict[letter] = param_dict


            for letter in sequence_letters:
                relevant_dict = block_sequence_filter_dict[letter]
                allow_smart_tags = True
                block_res = self.calculate_block_for_sequence(relevant_dict, allow_smart_tags)

                if len(block_res) > 1:
                    match_fk_list = set(match for cluster in block_res for node in cluster.nodes() for match in
                                        cluster.node[node]['match_fk'])

                    all_graph = AdjacencyGraph(self.matches, None, self.all_products,
                                               product_attributes=['rect_x', 'rect_y'],
                                               name=None, adjacency_overlap_ratio=.4)
                    # associate all nodes in the master graph to their associated match_fks
                    match_to_node = {int(node['match_fk']): i for i, node in
                                     all_graph.base_adjacency_graph.nodes(data=True)}
                    # create a dict of all match_fks to their corresponding nodes
                    node_to_match = {val: key for key, val in match_to_node.items()}
                    edge_matches = set(
                        sum([[node_to_match[i] for i in all_graph.base_adjacency_graph[match_to_node[match]].keys()]
                             for match in match_fk_list], []))
                    adjacent_matches = edge_matches - match_fk_list
                    adj_mpis = self.matches[(self.matches['scene_match_fk'].isin(adjacent_matches))]
                    block_adj_mpis[letter] = adj_mpis

            if len(block_adj_mpis.keys()) == len(sequence_letters):
                target_adj_mpis_df = block_adj_mpis[sequence_letters[0]].scene_match_fk.tolist()
                b_side_adj_mpis_df = block_adj_mpis[sequence_letters[1]].scene_match_fk.tolist()
                c_side_adj_mpis_df = block_adj_mpis[sequence_letters[2]].scene_match_fk.tolist()


                side_b_adj = len(set(target_adj_mpis_df).intersection(b_side_adj_mpis_df))
                side_c_adj = len(set(target_adj_mpis_df).intersection(c_side_adj_mpis_df))

                if side_b_adj > 0 and side_c_adj > 0:
                    custom_text = 'Yes'
                    result = 1
            custom_result_fk = Consts.CUSTOM_RESULTS[custom_text]

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                             numerator_result=result,
                             denominator_result=1, result=custom_result_fk, score=0)


    def calculate_block_for_sequence(self, relevant_dict, allow_smart_tags):

        product_fks = []
        if allow_smart_tags:
            product_fks = self.smart_tags_product_fks

        additional_block_params = {'minimum_facing_for_block': 2,
                                   'include_stacking': True,
                                   'allowed_products_filters': {'product_type': ['SKU','Other','Empty'], 'product_fk':product_fks}}

        block_result = self.block.network_x_block_together(relevant_dict, additional=additional_block_params)

        passed_blocks = block_result[block_result['is_block'] == True].cluster.tolist()

        return passed_blocks


    def calculate_blocking_comp(self):
        template = self.kpi_template[Consts.BLOCK_COMPOSITION_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['DATASET A PARAM 1']
            Value1 = self.sanitize_row(row['DATASET A VALUE 1'])

            reduced_scif = self.scif[~self.scif['Scent'].isnull()
                                     | (~self.scif['Format'].isnull())]

            for scent in reduced_scif['Scent'].tolist():
                filtered_scif = reduced_scif[reduced_scif['Scent'] == scent]
                product_fk = self.scif.product_fk.iloc[0]
                result = 0
                custom_result_fk = Consts.CUSTOM_RESULTS['No']
                block_result_list = []

                for scene in filtered_scif.scene_fk.unique().tolist():
                    general_filter = {}
                    filtered_scif = filtered_scif[filtered_scif['scene_fk'] == scene]
                    if not pd.isna(Params1):

                        general_filter[Params1] = Value1
                        general_filter['scene_fk'] = [scene]

                        block_result_df = self.block.network_x_block_together(population=general_filter,
                        additional={
                                    'minimum_facing_for_block': 1,
                                    'allowed_products_filters': {'product_type': 'sku'},

                                    'include_stacking': True})

                        passed_blocks = block_result_df[block_result_df['is_block'] == True].cluster.tolist()
                        # filtered_scif = filtered_scif[filtered_scif[Params1].isin(Value1)]

                        scent_res = 0

                        if len(passed_blocks) > 0:
                            match_fk_list = set(match for cluster in passed_blocks for node in cluster.nodes() for match in
                                                cluster.node[node]['match_fk'])
                            filtered_mpis = self.mpis[self.mpis['scene_match_fk'].isin(match_fk_list)]

                            format_count = len(filtered_mpis['Format'].unique())

                            if format_count > 1:
                                scent_res = 1

                        block_result_list.append(scent_res)

                if len(block_result_list) > 0:
                    acceptance_ratio = sum(block_result_list) / float(len(block_result_list))
                    if acceptance_ratio > .5:
                        result = 1
                        custom_result_fk = Consts.CUSTOM_RESULTS['Yes']

                self.write_to_db(fk=kpi_fk, numerator_id=product_fk, denominator_id=product_fk, numerator_result=result,
                                 denominator_result=1, result=custom_result_fk, score=0)


    def calculate_negative_max_blocking_adj(self):
        pass

    def calculate_max_blocking_adj(self):
        pass

    def calculate_blocking_for_max_block(self, filter_dict, max_block_type):
        pass

    def calculate_blocking(self):
        template = self.kpi_template[Consts.BLOCKING_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Value1 = row['VALUE 1']
            Params2 = row['PARAM 2']
            VALUE2 = row['VALUE 2']
            minimum_block_ratio = row['minimum_ratio']

            Exlude_Params1 = row['EXCLUDE PARAM 1']
            Exclude_Value1 = self.sanitize_row(row['EXCLUDE VALUE 1'])

            Exlude_Params2 = row['EXCLUDE PARAM 2']
            Exclude_Value2 = self.sanitize_row(row['EXCLUDE VALUE 2'])

            Block_AllowConnected_Params1 = row['BLOCK ALLOW-CONNECTED PARAM 1']
            Block_AllowConnected_Value1 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 1'])

            Block_AllowConnected_Params2 = row['BLOCK ALLOW-CONNECTED PARAM 2']
            Block_AllowConnected_Value2 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 2'])

            Block_AllowConnected_Params3 = row['BLOCK ALLOW-CONNECTED PARAM 3']
            Block_AllowConnected_Value3 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 3'])

            Block_Exlude_Params1 = row['BLOCK EXCLUDE PARAM 1']
            Block_Exclude_Value1 = self.sanitize_row(row['BLOCK EXCLUDE VALUE 1'])

            # is_aggregate = row['aggregate']
            #
            # if is_aggregate == 'yes':
            #     pass

            connect_dict = {}
            excluded_dict = {}
            smart_attribute_data_df = \
                self.hdp.get_match_product_in_probe_state_values(self.matches['probe_match_fk'].unique().tolist())
            smart_tags_product_fks = smart_attribute_data_df.product_fk.tolist()

            if Block_AllowConnected_Params1:
                connect_dict.update({Block_AllowConnected_Params1: Block_AllowConnected_Value1})
            if Block_AllowConnected_Params2:
                connect_dict.update({Block_AllowConnected_Params2: Block_AllowConnected_Value2})
            if Block_AllowConnected_Params3:
                connect_dict.update({Block_AllowConnected_Params3: Block_AllowConnected_Value3})

            connected_product_pks = []
            for key in connect_dict.keys():
                if key == 'Smart Tag':
                    connected_product_pks.extend(smart_tags_product_fks)
                elif not pd.isna(key):
                    filtered_dict = self.scif[self.scif[key].isin(connect_dict[key])]

                    if not filtered_dict.empty:
                        connected_product_pks.extend(filtered_dict.product_fk.tolist())

            result = 0

            param_dict = {Params1: [Value1], Params2: [VALUE2]}
            excluded_dict = {Exlude_Params1: Exclude_Value1, Exlude_Params2: Exclude_Value2,
                             Block_Exlude_Params1: Block_Exclude_Value1}

            # for param_key in param_dict.keys():
            #     if not pd.isna(param_key):
            #         general_filters[param_key] = param_dict[param_key]

            general_filters = self.remove_nans_dict(param_dict)
            excluded_filters = self.remove_nans_dict(excluded_dict)
            if Block_Exlude_Params1 == 'Smart Tag':
                excluded_filters['product_fk'] = smart_tags_product_fks
                del excluded_filters[Block_Exlude_Params1]

            block_result = self.block.network_x_block_together(
                population=general_filters,
                additional={'minimum_block_ratio': minimum_block_ratio,
                            'minimum_facing_for_block': 1,
                            'allowed_products_filters': {'product_type': connected_product_pks},
                            'include_stacking': True,
                            'exclude_filter': excluded_filters,
                            'check_vertical_horizontal': False})

            if not block_result.empty:
                result = 1

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=result,
                             denominator_id=self.store_id, result=result)

    def calculate_liner_measure(self):
        template = self.kpi_template[Consts.LINEAR_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            parent_brand_list = self.remove_type_none_from_list(self.scif[Params1].unique().tolist())
            relevant_scif = self.scif[self.scif[Params1].isin(parent_brand_list)]
            matches_no_stacking = self.matches[self.matches['stacking_layer'] == 1]
            mpis = pd.merge(relevant_scif, matches_no_stacking, how='left', on='product_fk')

            for parent_brand in parent_brand_list:
                for format in mpis[Params2][mpis[Params1] == parent_brand].unique().tolist():
                    if not pd.isna(format):
                        relevant_mpis = mpis[(mpis[Params2] == format) & (mpis[Params1] == parent_brand)]

                        linear_per_format_sum_mm = relevant_mpis['width_mm_advance'].sum()
                        product_fk = relevant_mpis.product_fk.iloc[0]

                        linear_per_format_sum_ft = linear_per_format_sum_mm * float(0.00328)
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=linear_per_format_sum_mm,
                                         context_id=product_fk,
                                         denominator_id=self.manufacturer_fk, result=linear_per_format_sum_ft)

    def calculate_base_measurement(self):
        template = self.kpi_template[Consts.BASE_MEASURE_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']

            sub_category_list = self.remove_type_none_from_list(self.scif.sub_category_fk.unique().tolist())
            mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')
            for sub_category_fk in sub_category_list:
                relevant_mpis = mpis[(mpis[Params1] == sub_category_fk) & (mpis['stacking_layer'] == 1)]
                bay_measure = []
                bay_mm_total = 0
                bays = relevant_mpis.bay_number.unique().tolist()
                for bay in bays:
                    shelves_in_bay = len(relevant_mpis['shelf_number'][relevant_mpis['bay_number'] == bay].unique())
                    mpis_by_bay_df = relevant_mpis[relevant_mpis['bay_number'] == bay]
                    bay_sum_width_mm = mpis_by_bay_df['width_mm_advance'].sum()
                    bay_avg = bay_sum_width_mm / float(shelves_in_bay)
                    bay_measure.append(bay_avg)
                    bay_mm_total += bay_avg
                bay_feet = bay_mm_total * 0.00328
                self.write_to_db(fk=kpi_fk,
                                 numerator_id=sub_category_fk, numerator_result=bay_mm_total,
                                 denominator_id=self.store_id, result=bay_feet)

    def calculate_smart_tags(self):
        template = self.kpi_template[Consts.SMART_TAG_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Value1 = row['VALUE 1']

            relevant_mpis = self.matches

            smart_attribute_data_df = \
                self.hdp.get_match_product_in_probe_state_values(relevant_mpis['probe_match_fk'].unique().tolist())

            result = 0

            smart_tags_df = pd.DataFrame()
            try:
                smart_tags_df = smart_attribute_data_df[smart_attribute_data_df[Params1] == Value1]
            except:
                pass

            if not smart_tags_df.empty:
                result = 1

            self.write_to_db(fk=kpi_fk,
                             numerator_id=self.manufacturer_fk, numerator_result=result,
                             denominator_id=self.store_id, result=result)

    def calculate_sku_count(self):
        template = self.kpi_template[Consts.SKU_COUNT_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            param1_list = self.remove_type_none_from_list(list(self.scif[Params1].unique()))
            param2_list = self.remove_type_none_from_list(list(self.scif[Params2].unique()))
            for value1 in param1_list:
                filtered_scif_df = self.scif[self.scif[Params1] == value1]
                for value2 in param2_list:
                    reduce_scif_df = filtered_scif_df[filtered_scif_df[Params2] == value2]
                    if not reduce_scif_df.empty:
                        product_fk = reduce_scif_df['product_fk'].iloc[0]
                        result = len(reduce_scif_df['product_fk'].unique())
                        manufacturer_fk = reduce_scif_df['manufacturer_fk'].iloc[0]
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=result,
                                         denominator_id=manufacturer_fk,
                                         context_id=product_fk, result=result)

    def calculate_facing_count(self):
        template = self.kpi_template[Consts.FACING_COUNT_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            param1_list = self.remove_type_none_from_list(list(self.scif[Params1].unique()))
            param2_list = self.remove_type_none_from_list(list(self.scif[Params2].unique()))
            for value1 in param1_list:
                filtered_scif_df = self.scif[self.scif[Params1] == value1]
                for value2 in param2_list:
                    reduce_scif_df = filtered_scif_df[filtered_scif_df[Params2] == value2]
                    if not reduce_scif_df.empty:
                        product_fk = reduce_scif_df['product_fk'].iloc[0]
                        result = reduce_scif_df['facings_ign_stack'].sum()
                        manufacturer_fk = reduce_scif_df['manufacturer_fk'].iloc[0]
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=result,
                                         denominator_id=manufacturer_fk,
                                         context_id=product_fk, result=result)

    def remove_type_none_from_list(self, orig_list):
        for item in orig_list:
            if pd.isna(item):
                try:
                    orig_list.remove(item)
                except:
                    pass
        fixed_list = orig_list
        return fixed_list

    def parse_template(self, template_path, relevant_sheets_list):
        template = {}
        if len(relevant_sheets_list) > 0:
            for sheet in relevant_sheets_list:
                template[sheet] = pd.read_excel(template_path, sheetname=sheet, encoding='utf8')

        return template

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df
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

    def sanitize_row(self, row):

        if type(row) == unicode:
            row = row.encode()
            items = row.split(",")
            cleansed_items = [s.strip() if type(s) == str else s for s in items]
            return cleansed_items
        else:
            return [row]

    def remove_nans_dict(self, input_dict):
        filtered_dict = {}
        for param_key in input_dict.keys():
            if not pd.isna(param_key):
                filtered_dict[param_key] = input_dict[param_key]
        return filtered_dict

    def calculate_smart_tags(self):
        smart_attribute_data_df = \
            self.hdp.get_match_product_in_probe_state_values(self.matches['probe_match_fk'].unique().tolist())
        self.smart_tags_product_fks = smart_attribute_data_df.product_fk.tolist()

    def _filter_redundant_edges(self, adj_g):
        """Since the edges determines by the masking only, there's a chance that there will be two edges
        that come out from a single node. This method filters the redundant ones (the ones who skip the
        closet adjecent node)"""
        edges_filter = []
        for node_fk, node_data in adj_g.nodes(data=True):
            for direction in ['UP', 'DOWN', 'LEFT', 'RIGHT']:
                edges = [(edge[0], edge[1]) for edge in list(adj_g.edges(node_fk, data=True)) if
                         edge[2]['direction'] == direction]
                if len(edges) <= 1:
                    edges_filter.extend(edges)
                else:
                    edges_filter.append(self._get_shortest_path(adj_g, edges))
        return edges_filter

    @staticmethod
    def _get_product_fks_with_filter(scif, input_dict):
        for column, value in input_dict.items():
            scif = scif[scif[column].isin(value)]
        return scif.product_fk.unique()

    def _get_shortest_path(self, adj_g, edges_to_check):
        """ This method gets a list of edge and returns the one with the minimum distance"""
        distance_per_edge = {edge: self._get_edge_distance(adj_g, edge) for edge in edges_to_check}
        shortest_edge = min(distance_per_edge, key=distance_per_edge.get)
        return shortest_edge

    def _get_edge_distance(self, adj_g, edge):
        """
        This method gets an edge and calculate it's length (the distance between it's nodes)
        """
        first_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[0]))
        second_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[1]))
        distance = np.sqrt(np.sum((first_node_coordinate - second_node_coordinate) ** 2))
        return distance

    def _get_node_display_coordinates(self, adj_g, node_fk):
        """
        This method gets a node and extract the Display coordinates (x and y).
        Those attributes were added to each node since this is the attributes we condensed the graph by
        """
        return float(list(adj_g.nodes[node_fk]['rect_x'])[0]), float(list(adj_g.nodes[node_fk]['rect_y'])[0])


    def _get_block(self, filters, additional_filter):

        block = self.block.network_x_block_together(filters,additional=additional_filter)
        if not block.empty:
            block = block[block.is_block == True]
        return block

    def _get_adjacent_node_direction(self, dataseta_block, datasetb_block):
        result_dict = {'LEFT':1, 'DOWN':2, 'RIGHT':3, 'UP':4}
        result = 0
        for b_index, block_b_row in datasetb_block.iterrows():
            valid_cluster_for_blockb = block_b_row.cluster.nodes.values()
            relevant_scene_match_fks_for_block_b = [scene_match_fk for item in valid_cluster_for_blockb for
                                                    scene_match_fk in item['scene_match_fk']]

            for a_index, block_a_row in dataseta_block.iterrows():
                valid_cluster_for_blocka = block_a_row.cluster.nodes.values()
                relevant_scene_match_fks_for_block_a = [scene_match_fk for item in valid_cluster_for_blocka for
                                                        scene_match_fk in item['scene_match_fk']]
                '''The below line is used to filter the adjacency graph by the closest edges. Specifically since
                                     the edges are determined by masking only, there's a chance that there will be two edges that come
                                     out from a single node.'''
                adj_graph = self.block.adj_graphs_by_scene.values()[0].edge_subgraph(
                    self._filter_redundant_edges(self.block.adj_graphs_by_scene.values()[0]))
                for scene_match_a in relevant_scene_match_fks_for_block_a:
                    for node, node_data in adj_graph.adj[scene_match_a].items():
                        if node in relevant_scene_match_fks_for_block_b:
                            result_direction = node_data['direction']
                            result = result_dict[result_direction]
                            return result
        return result

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row['KPI Name']
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        return kpi_name, kpi_fk


