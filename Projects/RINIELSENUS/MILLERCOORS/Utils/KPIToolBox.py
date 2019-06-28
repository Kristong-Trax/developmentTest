from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.BlockCalculations import Block
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.RINIELSENUS.MILLERCOORS.Data.Const import Const

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Miller Coors KPI Template_v1.xlsx')


class MILLERCOORSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.template_info = self.data_provider.all_templates
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.template_info, on='template_fk', suffixes=['', '_t'])
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.custom_entity_data = self.get_custom_entity_data()
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates = {}
        for sheet in Const.SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet).fillna('')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        # main_template = self.templates[Const.KPIS]
        # self.templates[Const.KPIS] = main_template[main_template[Const.STORE_TYPE] == self.store_type]
        self.block = Block(self.data_provider, self.output, common=self.common)
        self.adjacency = Adjancency(self.data_provider, self.output, common=self.common)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # self.calculate_block_adjacency(None, None)
        main_template = self.templates[Const.KPIS]
        for i, main_line in main_template.iterrows():
            relevant_scif = self.scif[self.scif['product_type'] != 'Empty']
            kpi_name = main_line[Const.KPI_NAME]
            kpi_type = main_line[Const.KPI_TYPE]
            scene_types = self.does_exist(main_line, Const.STORE_LOCATION)
            if scene_types:
                relevant_scif = self.scif[self.scif['template_name'].isin(scene_types)]
                if relevant_scif.empty:
                    if main_line[Const.WRITE_NA] == 'Y':
                        # we need to save a N/A result if the KPI is not going to run
                        # it's easier to implement the logic here rather than in each KPI
                        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_name,
                                                                          numerator_id=999,
                                                                          numerator_result=2,
                                                                          result=2,
                                                                          denominator_id=0,
                                                                          denominator_result=2)
                        self.common.write_to_db_result(**result_dict)
                    continue

            relevant_template = self.templates[kpi_type]
            relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
            relevant_template = relevant_template.merge(main_template, how='left', left_on=Const.KPI_NAME,
                                                        right_on=Const.KPI_NAME)
            kpi_function = self.get_kpi_function(kpi_type)
            for idx, kpi_line in relevant_template.iterrows():
                kpi_function(kpi_line, relevant_scif)
        return

    def calculate_anchor(self, kpi_line, relevant_scif):

        filters = {
            kpi_line[Const.PARAM]: kpi_line[Const.VALUE],
            'template_name': kpi_line[Const.STORE_LOCATION]
        }

        passed_scenes, total_scenes = self.calculate_products_on_edge(min_number_of_facings=1,
                                                                      min_number_of_shelves=1,
                                                                      **filters)

        template_fk = relevant_scif['template_fk'].values[0]
        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                          numerator_id=999, numerator_result=int(passed_scenes > 0),
                                                          result=int(passed_scenes > 0), denominator_id=template_fk,
                                                          denominator_result=1)
        self.common.write_to_db_result(**result_dict)

        return

    def calculate_block(self, kpi_line, relevant_scif):
        kpi_result = 0
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            location_filter = {'scene_id': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            # allowed = {'product_type': ['Other', 'Empty']}
            filters = {kpi_line[Const.PARAM]: kpi_line[Const.VALUE]}
            items = set(self.filter_df(mpis, filters)['scene_match_fk'].values)
            additional = {'minimum_facing_for_block': 2}
            # allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            if not (items):
                break

            block_result = self.block.network_x_block_together(filters, location=location_filter, additional=additional)

            passed_blocks = block_result[block_result['is_block'] == True].cluster.tolist()

            if passed_blocks:
                kpi_result = 1
                break

        template_fk = relevant_scif['template_fk'].values[0]
        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                          numerator_id=999, numerator_result=kpi_result,
                                                          result=kpi_result, denominator_id=template_fk,
                                                          denominator_result=1)
        self.common.write_to_db_result(**result_dict)

        return

    def calculate_adjacency(self, kpi_line, relevant_scif):
        kpi_result = 0
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            mpis = mpis[mpis['stacking_layer'] >= 1]
            # allowed = {'product_type': ['Other', 'Empty']}
            filters = {kpi_line[Const.ANCHOR_PARAM]: kpi_line[Const.ANCHOR_VALUE]}
            # determine if there are any matching products in the scene
            items = set(self.filter_df(mpis, filters)['scene_match_fk'].values)
            # allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            # items.update(allowed_items)
            if not (items):
                break

            all_graph = AdjacencyGraph(mpis, None, self.products,
                                       product_attributes=['rect_x', 'rect_y'],
                                       name=None, adjacency_overlap_ratio=.4)

            match_to_node = {int(node['match_fk']): i for i, node in all_graph.base_adjacency_graph.nodes(data=True)}
            node_to_match = {val: key for key, val in match_to_node.items()}
            edge_matches = set(
                sum([[node_to_match[i] for i in all_graph.base_adjacency_graph[match_to_node[item]].keys()]
                     for item in items], []))
            adjacent_items = edge_matches - items
            adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items)) &
                            (~mpis['product_type'].isin(['Empty', 'Irrelevant', 'Other', 'POS']))]

            if kpi_line[Const.LIST_ATTRIBUTE]:
                for value in adj_mpis[kpi_line[Const.LIST_ATTRIBUTE]].unique().tolist():
                    if kpi_line[Const.LIST_ATTRIBUTE] == 'brand_name':
                        numerator_fk = adj_mpis[adj_mpis['brand_name'] == value].brand_fk.values[0]
                    else:
                        if value is not None:
                            try:
                                numerator_fk = \
                                self.custom_entity_data[self.custom_entity_data['name'] == value].pk.values[0]
                            except IndexError:
                                Log.warning('Custom entity "{}" does not exist'.format(value))
                                continue
                        else:
                            continue

                    result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                                      numerator_id=numerator_fk, numerator_result=1,
                                                                      result=1, denominator_id=scene,
                                                                      denominator_result=1)
                    self.common.write_to_db_result(**result_dict)
                return
            else:
                if kpi_line[Const.TESTED_VALUE] in adj_mpis[kpi_line[Const.TESTED_PARAM]].unique().tolist():
                    kpi_result = 1
                    break

        if kpi_line[Const.LIST_ATTRIBUTE]:  # handle cases where there are no relevant products,
            return                          # so we miss the other check above
        template_fk = relevant_scif['template_fk'].values[0]
        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                          numerator_id=999, numerator_result=kpi_result,
                                                          result=kpi_result, denominator_id=template_fk,
                                                          denominator_result=1)
        self.common.write_to_db_result(**result_dict)

    def calculate_block_adjacency(self, kpi_line, relevant_scif):
        kpi_result = 0
        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            location_filter = {'scene_id': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            mpis = mpis[mpis['stacking_layer'] >= 1]
            # allowed = {'product_type': ['Other', 'Empty']}
            if kpi_line[Const.TESTED_PARAM] == kpi_line[Const.ANCHOR_PARAM]:
                filters = {kpi_line[Const.ANCHOR_PARAM]: [kpi_line[Const.ANCHOR_VALUE], kpi_line[Const.TESTED_VALUE]]}
            elif kpi_line[Const.TESTED_PARAM] == '':
                filters = {kpi_line[Const.ANCHOR_PARAM]: kpi_line[Const.ANCHOR_VALUE]}
            else:
                filters = {kpi_line[Const.ANCHOR_PARAM]: kpi_line[Const.ANCHOR_VALUE],
                           kpi_line[Const.TESTED_PARAM]: kpi_line[Const.TESTED_VALUE]}
            items = set(self.filter_df(mpis, filters)['scene_match_fk'].values)
            additional = {'minimum_facing_for_block': 2}
            # allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            if not (items):
                break

            block_result = self.block.network_x_block_together(filters, location=location_filter, additional=additional)

            passed_blocks = block_result[block_result['is_block'] == True].cluster.tolist()

            if passed_blocks and kpi_line[Const.LIST_ATTRIBUTE]:
                match_fk_list = set(match for cluster in passed_blocks for node in cluster.nodes() for match in
                                    cluster.node[node]['group_attributes']['match_fk_list'])

                all_graph = AdjacencyGraph(mpis, None, self.products,
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
                adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_matches)) &
                                (~mpis['product_type'].isin(['Empty', 'Irrelevant', 'Other', 'POS']))]

                for value in adj_mpis[kpi_line[Const.LIST_ATTRIBUTE]].unique().tolist():
                    if kpi_line[Const.LIST_ATTRIBUTE] == 'brand_name':
                        numerator_fk = adj_mpis[adj_mpis['brand_name'] == value].brand_fk.values[0]
                    else:
                        if value is not None:
                            try:
                                numerator_fk = \
                                self.custom_entity_data[self.custom_entity_data['name'] == value].pk.values[0]
                            except IndexError:
                                Log.warning('Custom entity "{}" does not exist'.format(value))
                                continue
                        else:
                            continue

                    result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                                      numerator_id=numerator_fk, numerator_result=1,
                                                                      result=1, denominator_id=scene,
                                                                      denominator_result=1)
                    self.common.write_to_db_result(**result_dict)
                return
            elif kpi_line[Const.LIST_ATTRIBUTE]:  # return if this is a list_attribute KPI with no passing blocks
                return
            if passed_blocks:  # exit loop if this isn't a list_attribute KPI, but has passing blocks
                kpi_result = 1
                break
        if kpi_line[Const.LIST_ATTRIBUTE]:  # handle cases where there are no relevant products,
            return                          # so we miss the other check above
        template_fk = relevant_scif['template_fk'].values[0]
        result_dict = self.build_dictionary_for_db_insert(kpi_name=kpi_line[Const.KPI_NAME],
                                                          numerator_id=999, numerator_result=kpi_result,
                                                          result=kpi_result, denominator_id=template_fk,
                                                          denominator_result=1)
        self.common.write_to_db_result(**result_dict)
        return

    def calculate_products_on_edge(self, min_number_of_facings=1, min_number_of_shelves=1, **filters):
        """
        :param min_number_of_facings: Minimum number of edge facings for KPI to pass.
        :param min_number_of_shelves: Minimum number of different shelves with edge facings for KPI to pass.
        :param filters: This are the parameters which dictate the relevant SKUs for the edge calculation.
        :return: A tuple: (Number of scenes which pass, Total number of relevant scenes)
        """
        filters, relevant_scenes = self.separate_location_filters_from_product_filters(**filters)
        if len(relevant_scenes) == 0:
            return 0, 0
        number_of_edge_scenes = 0
        for scene in relevant_scenes:
            edge_facings = pd.DataFrame(columns=self.mpis.columns)
            matches = self.mpis[self.mpis['scene_fk'] == scene]
            for shelf in matches['shelf_number'].unique():
                shelf_matches = matches[matches['shelf_number'] == shelf]
                if not shelf_matches.empty:
                    shelf_matches = shelf_matches.sort_values(by=['bay_number', 'facing_sequence_number'])
                    edge_facings = edge_facings.append(shelf_matches.iloc[0])
                    if len(edge_facings) > 1:
                        edge_facings = edge_facings.append(shelf_matches.iloc[-1])
            edge_facings = edge_facings[self.get_filter_condition(edge_facings, **filters)]
            if len(edge_facings) >= min_number_of_facings \
                    and len(edge_facings['shelf_number'].unique()) >= min_number_of_shelves:
                number_of_edge_scenes += 1
        return number_of_edge_scenes, len(relevant_scenes)

    def separate_location_filters_from_product_filters(self, **filters):
        """
        This function gets scene-item-facts filters of all kinds, extracts the relevant scenes by the location filters,
        and returns them along with the product filters only.
        """
        location_filters = {}
        for field in filters.keys():
            if field not in self.all_products.columns and field in self.scif.columns:
                location_filters[field] = filters.pop(field)
        relevant_scenes = self.scif[self.get_filter_condition(self.scif, **location_filters)]['scene_id'].unique()
        return filters, relevant_scenes

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

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUJTIUAGENERALToolBox.INCLUDE_FILTER)
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

    def get_kpi_function(self, kpi_type):
        """
        transfers every kpi to its own function
        :param kpi_type: value from "sheet" column in the main sheet
        :return: function
        """
        if kpi_type == Const.ANCHOR:
            return self.calculate_anchor
        elif kpi_type == Const.BLOCK:
            return self.calculate_block
        elif kpi_type == Const.BLOCK_ADJACENCY:
            return self.calculate_block_adjacency
        elif kpi_type == Const.ADJACENCY:
            return self.calculate_adjacency
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
                return [cell]
        return None

    def build_dictionary_for_db_insert(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                       denominator_id=0, denominator_result=0, score=0, score_after_actions=0,
                                       denominator_result_after_actions=None, numerator_result_after_actions=0,
                                       weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=None,
                                       target=None,
                                       identifier_parent=None, identifier_result=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.common.get_kpi_fk_by_kpi_name(kpi_name)
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
                insert_params['should_enter'] = True
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None

    def get_custom_entity_data(self):
        query = """
                select *
                from static.custom_entity
                """
        custom_entity_data = pd.read_sql_query(query, self.rds_conn.db)
        return custom_entity_data

    def commit_results(self):
        self.common.commit_results_data()
