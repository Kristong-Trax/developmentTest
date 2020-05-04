import numpy as np
import networkx as nx
from collections import Counter
from Trax.Cloud.Services.Connector.Logger import Log
from Projects.DIAGEOUS_SAND2.Data.LocalConsts import CaseCountConsts as Ccc
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts as Mc, ProductsConsts as Pc, ScifConsts as Sc
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts as Src
from consts import Consts


class CaseCountCalculator(GlobalSessionToolBox):

    def __init__(self, data_provider, common):
        GlobalSessionToolBox.__init__(self, data_provider, None)
        self.filtered_mdis = self._get_filtered_match_display_in_scene()
        self.filtered_scif = self._get_filtered_scif()
        self.matches = self.get_filtered_matches()
        self.common = common

    def _get_filtered_match_display_in_scene(self):
        """ This method filters match display in scene - it saves only "close" and "open" tags"""
        mdis = self.data_provider.match_display_in_scene.loc[
            self.data_provider.match_display_in_scene.display_name.str.contains(Consts.RELEVANT_DISPLAYS_SUFFIX)]
        return mdis

    def main_case_count_calculations(self):
        """This method calculates the entire Case Count KPIs set."""
        if self.filtered_mdis.empty or self.filtered_scif.empty:
            return
        try:
            self._prepare_data_for_calculation()
            total_facings_res = self._calculate_display_size_facings()
            cases_per_sku_res = self._count_number_of_cases()
            unshoppable_cases_res = self._non_shoppable_case_kpi()
            implied_shoppable_res = self._implied_shoppable_cases_kpi()
            total_cases_res = self._calculate_and_total_cases(cases_per_sku_res + implied_shoppable_res)
            # self._save_results_to_db(total_cases_res, Ccc.TOTAL_CASES_KPI, should_enter=False)
        except Exception as err:
            Log.error("DiageoUS Case Count calculation failed due to the following error: {}".format(err))

    @staticmethod
    def _calculate_and_total_cases(kpi_results):
        """ This method sums # of cases per brand and Implied Shoppable Cases KPIs
        and saves the main result to the DB"""
        total_results_per_sku, results_list = Counter(), list()
        for res in kpi_results:
            total_results_per_sku[res[Pc.PRODUCT_FK]] += res[Src.RESULT]
        for product_fk, result in total_results_per_sku.iteritems():
            kpi_id = '{}_{}'.format(product_fk, Ccc.TOTAL_CASES_KPI)
            results_list.append({Pc.PRODUCT_FK: product_fk, Src.RESULT: result, 'kpi_id': kpi_id})
        return results_list

    def _save_results_to_db(self, results_list, kpi_name, result_key=Src.RESULT, should_enter=True):
        """This method saves the KPI results to DB"""
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        for res in results_list:
            kpi_id = res.get('kpi_id', None)
            parent_id = '{}_{}'.format(res[Pc.PRODUCT_FK], Ccc.TOTAL_CASES_KPI) if should_enter else None
            result = res[result_key]
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=res[Pc.PRODUCT_FK],
                                           numerator_result=result, result=result, denominator_id=self.store_id,
                                           denominator_result=0, score=result, identifier_result=kpi_id,
                                           identifier_parent=parent_id, should_enter=should_enter)

    def _prepare_data_for_calculation(self):
        """This method prepares the data for the case count calculation. Connection between the display
        data and the tagging data."""
        closest_tag_to_display_df = self._calculate_closest_product_to_display()
        self._add_displays_the_closet_product_fk(closest_tag_to_display_df)
        self._add_matches_display_data(closest_tag_to_display_df)

    def _calculate_closest_product_to_display(self):
        """This method calculates the closest tag for each display and returns a DataFrame with the results"""
        matches = self.matches[Consts.RLV_FIELDS_FOR_MATCHES_CLOSET_DISPLAY_CALC]
        mdis = self.filtered_mdis[Consts.RLV_FIELDS_FOR_DISPLAY_IN_SCENE_CLOSET_TAG_CALC]
        matches[Consts.DISPLAY_IN_SCENE_FK] = matches.apply(
            lambda row: self._apply_closet_point_logic_on_row(row, mdis, 'pk'), axis=1)
        return matches[[Mc.SCENE_MATCH_FK, Consts.DISPLAY_IN_SCENE_FK]]

    def _calculate_display_size_facings(self):
        """This method calculates the number of facings for SKUs with the relevant SKU Type
        only in scenes that have displays"""
        filtered_scif = self.filtered_scif.loc[(self.filtered_scif[Sc.SKU_TYPE].isin(Consts.FACINGS_SKU_TYPES))]
        filtered_scif = filtered_scif.loc[filtered_scif[Sc.TAGGED] > 0][[Pc.PRODUCT_FK, Sc.TAGGED]]
        results_df = filtered_scif.groupby(Pc.PRODUCT_FK, as_index=False).sum().rename({Sc.TAGGED: Src.RESULT}, axis=1)
        results_df = results_df.assign(fk=self.get_kpi_fk_by_kpi_type(Consts.FACINGS_KPI))
        return results_df.to_dict('records')

    def _count_number_of_cases(self):
        """This method counts the number of cases per SKU.
        It identify the closest SKU tag to every case and using it to define the case's brand
        """
        results_df = self.filtered_mdis.groupby(Consts.DISPLAY_SKU, as_index=False)[Consts.DISPLAY_SKU].agg(
            {'result': 'count'})
        results_df.rename({Consts.DISPLAY_SKU: Pc.PRODUCT_FK}, axis=1, inplace=True)
        results_df = results_df.assign(fk=self.get_kpi_fk_by_kpi_type(Consts.SHOPPABLE_CASES_KPI))
        return results_df.to_dict('records')

    def _implied_shoppable_cases_kpi(self):
        """
        This method calculates the implied shoppable cases KPI. It creates Adjacency graph per scene,
        iterates paths over the cases pile and count the number of hidden cases.
        """
        results = []
        scenes_to_calculate = self.matches.scene_fk.unique().tolist()
        total_score_per_sku = Counter()
        for scene_fk in scenes_to_calculate:
            adj_g = self._create_adjacency_graph_per_scene(scene_fk)
            paths = self._get_relevant_path_for_calculation(adj_g)
            total_score_per_sku += self._calculate_case_count(adj_g, paths)
        for k, v in total_score_per_sku.iteritems():
            results.append({Pc.PRODUCT_FK: k, 'result': v})
        results += self._add_results_for_brands_without_hidden_displays(results)
        return results

    def _add_results_for_brands_without_hidden_displays(self, results):
        """ This method gets the _implied_shoppable_cases_kpi and adds all of the brand that has displays assigned
        to them, however there aren't any hidden displays at all (result should be 0)"""
        products_with_results = [res[Pc.PRODUCT_FK] for res in results]
        total_products_with_displays = self.filtered_mdis[Consts.DISPLAY_SKU].dropna().unique().tolist()
        missing_products = set(total_products_with_displays).difference(products_with_results)
        new_results = [{Pc.PRODUCT_FK: product, Src.RESULT: 0} for product in missing_products]
        return new_results

    def _non_shoppable_case_kpi(self):
        """ This method calculates the number of unshoppable SKUs.
        SKU is considered non shoppable if there are only 'case' SKU Type without any facings' relevant one.
        Please note that if SKU is unshoppable it will get a result of 100!!"""
        unshoppable_results = []
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.NON_SHOPPABLE_CASES_KPI)
        grouped_scif = self.filtered_scif.groupby([Pc.PRODUCT_FK, Sc.SKU_TYPE], as_index=False)[Sc.TAGGED].sum()
        grouped_scif = grouped_scif.loc[grouped_scif.tagged > 0]
        grouped_scif_dict = grouped_scif.groupby(Pc.PRODUCT_FK)[Sc.SKU_TYPE].apply(list).to_dict()
        for product_fk, sku_types in grouped_scif_dict.iteritems():
            res = 100 if len(sku_types) == 1 and sku_types[0].lower() == 'case' else 0
            unshoppable_results.append({Pc.PRODUCT_FK: product_fk, Src.RESULT: res, 'fk': kpi_fk})
        return unshoppable_results

    @staticmethod
    def get_closest_point(origin_point, other_points_df):
        """This method gets a point (x & y coordinates) and checks what is the closet point the could be
        found in the DataFrame that is being received.
        @param: origin_point (tuple): coordinates of x and y
        @param: other_points_df (DataFrame): A DF that has rect_x' and 'rect_y' columns
        @return: A DataFrame with the closest points and the rest of the data
        """
        other_points = other_points_df[['rect_x', 'rect_y']].values
        # Euclidean geometry magic
        distances = np.sum((other_points - origin_point) ** 2, axis=1)
        # get the shortest hypotenuse
        try:
            closest_point = other_points[np.argmin(distances)]
        except ValueError:
            Log.error('Unable to find a matching opposite point for supplied anchor!')
            return other_points_df
        return other_points_df[
            (other_points_df['rect_x'] == closest_point[0]) & (other_points_df['rect_y'] == closest_point[1])]

    def _apply_closet_point_logic_on_row(self, row, mdis, value_to_return='pk'):
        """
        This method gets a Series and filtered Match Display In Scene DataFrame and returns the closet point
        to the point that can be found in the row
        """
        current_point = (row['rect_x'], row['rect_y'])
        relevant_mdis = mdis.loc[(mdis.scene_fk == row[Sc.SCENE_FK])]
        closet_point = self.get_closest_point(current_point, relevant_mdis)
        if not closet_point.empty:
            return closet_point.iloc[0][value_to_return]
        return -1

    def _add_matches_display_data(self, closest_tag_to_display_df):
        """
        This method calculates the closets match_display_in_scene tag per row and adds
        it to Match Product in Scene DataFrame
        """
        mdis = self.filtered_mdis[[Consts.DISPLAY_IN_SCENE_FK, 'rect_x', 'rect_y', 'display_name', Consts.DISPLAY_SKU]]
        mdis = mdis.merge(closest_tag_to_display_df, how='left', on=Consts.DISPLAY_IN_SCENE_FK)
        mdis.rename({'rect_x': 'display_rect_x', 'rect_y': 'display_rect_y'}, inplace=True, axis=1)
        # todo finish the jon
        self.matches = self.matches.merge(mdis, on=Consts.DISPLAY_IN_SCENE_FK, how='left')
        # self.matches = self.matches.merge(closest_tag_to_display_df, on=[Mc.SCENE_MATCH_FK, Mc.SCENE_FK], how='left')
        # filtered_matches[Consts.DISPLAY_IN_SCENE_FK] = filtered_matches.apply(
        #     lambda row: self._apply_closet_point_logic_on_row(row, self.filtered_mdis),
        #     axis=1)

    def _add_displays_the_closet_product_fk(self, closest_tag_to_display_df):
        """
        This method calculates the closets match_display_in_scene tag per row and adds
        it to Match Product in Scene DataFrame
        """
        temp_matches = self.matches[[Mc.SCENE_MATCH_FK, Mc.PRODUCT_FK]]
        temp_display_with_closest_sku = closest_tag_to_display_df.merge(temp_matches, how='left', on=Mc.SCENE_MATCH_FK)
        temp_display_with_closest_sku.rename({Mc.PRODUCT_FK: Consts.DISPLAY_SKU}, axis=1, inplace=True)
        self.filtered_mdis = self.filtered_mdis.merge(
            temp_display_with_closest_sku[[Consts.DISPLAY_IN_SCENE_FK, Consts.DISPLAY_SKU]],
            right_on=Consts.DISPLAY_IN_SCENE_FK, left_on='pk', how='left')

    def _get_scenes_with_relevant_displays(self):
        """This method returns only scene with "Open" or "Close" display tags"""
        return self.filtered_mdis.scene_fk.unique().tolist()

    def get_filtered_matches(self):
        """ This method filters and merges Match Product In Scene and Match Display In Scene DataFrames"""
        scenes_with_display = self._get_scenes_with_relevant_displays()
        filtered_matches = self.data_provider.matches.loc[self.data_provider.matches.scene_fk.isin(scenes_with_display)]
        return filtered_matches

    @staticmethod
    def _filter_edges_by_degree(adj_g, requested_direction):
        """
        This method filters the edges by the relevant degree.
        :param requested_direction: 'RIGHT', 'LEFT', 'UP, 'BOTTOM'
        """
        degree_direction_dict = {'DOWN': range(-134, -44), 'UP': range(60, 130)}
        relevant_range = degree_direction_dict[requested_direction]
        valid_edges = [(u, v) for u, v, c in adj_g.edges.data('degree') if int(c) in relevant_range]
        return valid_edges

    def _filter_redundant_edges(self, adj_g):
        """Since the edges determines by the masking only, there's a chance that there will be two edges
        that come out from a single node. This method filters the redundant ones (the ones who skip the
        closet adjecent node)"""
        edges_filter = []
        for node_fk, node_data in adj_g.nodes(data=True):
            edges = list(adj_g.edges(node_fk))
            if len(edges) <= 1:
                edges_filter.extend(edges)
            else:
                edges_filter.append(self._get_shortest_path(adj_g, edges))
        return edges_filter

    def _get_shortest_path(self, adj_g, edges_to_check):
        """ This method gets a list of edge and returns the one with the minimum distance"""
        distance_per_edge = {edge: self._get_egde_distance(adj_g, edge) for edge in edges_to_check}
        shortest_edge = min(distance_per_edge, key=distance_per_edge.get)
        return shortest_edge

    def _get_egde_distance(self, adj_g, edge):
        """
        This method gets an edge and calculate it's length (the distance between it's nodes)
        """
        first_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[0]))
        second_node_coordinate = np.array(self._get_node_display_coordinates(adj_g, edge[1]))
        distance = np.sqrt(np.sum((first_node_coordinate - second_node_coordinate) ** 2))
        return distance

    @staticmethod
    def _get_node_display_coordinates(adj_g, node_fk):
        """
        This method gets a node and extract the Display coordinates (x and y).
        Those attributes were added to each node since this is the attributes we condensed the graph by
        """
        return float(list(adj_g.nodes[node_fk]['display_rect_x'])[0]), float(
            list(adj_g.nodes[node_fk]['display_rect_y'])[0])

    def _create_adjacency_graph_per_scene(self, scene_fk):
        """ This method creates the graph for the case count calculation"""
        filtered_matches = self._prepare_matches_for_graph_creation(scene_fk)
        if filtered_matches.empty:
            return None
        maskings = AdjacencyGraphBuilder._load_maskings(self.project_name, scene_fk)
        add_node_attr = [Consts.DISPLAY_IN_SCENE_FK, 'display_rect_x', 'display_rect_y', 'display_name', Consts.DISPLAY_SKU]
        adj_g = AdjacencyGraphBuilder.initiate_graph_by_dataframe(filtered_matches,
                                                                  maskings, add_node_attr, use_masking_only=True)
        adj_g = AdjacencyGraphBuilder.condense_graph_by_level(Consts.DISPLAY_IN_SCENE_FK, adj_g)
        filtered_adj_g = adj_g.edge_subgraph(self._filter_edges_by_degree(adj_g, requested_direction='UP'))
        filtered_adj_g = adj_g.edge_subgraph(self._filter_redundant_edges(filtered_adj_g))
        return filtered_adj_g

    def _prepare_matches_for_graph_creation(self, scene_fk):
        """ This method prepares the Matches DataFrame for the Adjacency Graph creation.
        Adding Displays data and adjust the columns"""
        scene_matches = self.matches.loc[self.matches.scene_fk == scene_fk]
        scene_matches = scene_matches.loc[scene_matches.stacking_layer > 0]  # For the Graph Creation
        scene_matches.drop('pk', axis=1, inplace=True)
        scene_matches.rename({'scene_match_fk': 'pk'}, axis=1, inplace=True)
        return scene_matches

    @staticmethod
    def _get_relevant_path_for_calculation(filtered_adj_g):
        """ This method returns the relevant paths to check from the adj_g.
        First, it extracts the bottom node and then filter only the relevant paths to calculates"""
        bottom_nodes = []
        if filtered_adj_g is None:
            return bottom_nodes
        for node_fk, node_data in filtered_adj_g.nodes(data=True):
            if not filtered_adj_g.in_edges(node_fk):
                bottom_nodes.append(node_fk)
        all_paths = list(nx.all_pairs_dijkstra_path(filtered_adj_g))
        relevant_paths = [max(path[1].values(), key=len) for path in all_paths if path[0] in bottom_nodes]
        return relevant_paths

    def _calculate_case_count(self, adj_g, paths_to_check):
        """
        This method gets the filtered adjacency graph and the path of the cases (every path is an isle???)
        It iterates it and calculated by the following logic: (X=hidden, O = open, C = closed)
        O
        XC      In this case the score will be 2
        XC
        """
        case_count_score = Counter()
        for path in paths_to_check:
            open_detection_indicator = False
            for stacking_layer, node in enumerate(path):
                case_status = self._get_case_status(adj_g.nodes[node])
                if not case_status:
                    continue  # Closed case
                else:
                    if open_detection_indicator:
                        current_sku = list(adj_g.nodes[node][Consts.DISPLAY_SKU])[0]
                        case_count_score[current_sku] += stacking_layer
                    else:
                        open_detection_indicator = True
        return case_count_score

    @staticmethod
    def _get_case_status(node_data):
        """This method checks if a case is open or closed by checking the node's attributes
        @return: 0 in case of close, 1 otherwise
        """
        return 0 if 'close' in list(node_data['display_name'])[0].lower() else 1

    def _get_filtered_scif(self):
        """The case count is relevant only for scene that includes specific displays.
        So this method filters scene item facts to support the case count logic."""
        scenes_with_display = self._get_scenes_with_relevant_displays()
        scif = self.data_provider.scene_item_facts.loc[
            self.data_provider.scene_item_facts.scene_fk.isin(scenes_with_display) & ~(
                self.data_provider.scene_item_facts[Sc.SKU_TYPE].isna())]
        return scif


from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
if __name__ == '__main__':
    Config.init('')
    data_provider = KEngineDataProvider('diageous-sand2')
    sessions = ['566FD433-FD02-4C23-95F8-CD26D8BA1A61']
    for session in sessions:
        print(session)
        data_provider.load_session_data(session_uid=session)
        # data_provider.load_scene_data(session_uid='10A57E97-BFE0-4295-8905-675603CFBF40', scene_id=4720834)
        # data_provider.load_scene_data(session_uid='4590C8B2-CD2E-4679-9BC7-CD31FA9FEF3D', scene_id=3851296)
        # data_provider.load_scene_data(session_uid='E53E363F-E824-45E5-BE05-A528BAF54E0B', scene_id=4575034)
        # data_provider.load_scene_data(session_uid='25434CF3-24EB-4E3E-BED8-DAFC9593031B', scene_id=4270665)
        # data_provider.load_scene_data(session_uid='25d12ef4-ff53-449e-87df-317f8d4db068', scene_id=3977871)
        # data_provider.load_scene_data(session_uid='c8f7ad4a-3867-4ffa-9bb2-80c4afa8daae', scene_id=3214683)
        # data_provider.load_scene_data(session_uid='10A57E97-BFE0-4295-8905-675603CFBF40', scene_id=4720826)
        # data_provider.load_scene_data(session_uid='10A57E97-BFE0-4295-8905-675603CFBF40', scene_id=4720871)
        # data_provider.load_scene_data(session_uid='E53E363F-E824-45E5-BE05-A528BAF54E0B', scene_id=4574982)
        # data_provider.load_scene_data(session_uid='6D8E1FF0-193E-414A-BCA6-1B5BBDE7681C', scene_id=3350018)
        # data_provider.load_scene_data(session_uid='EAEF7FE8-9C46-4794-B97B-507794912875', scene_id=4245012)
        # data_provider.load_scene_data(session_uid='E53E363F-E824-45E5-BE05-A528BAF54E0B', scene_id=4575166)
        # data_provider.load_scene_data(session_uid='C4465263-56B2-4A1D-B16B-D7F7C954ED6F', scene_id=4357090)
        # data_provider.load_scene_data(session_uid='E53E363F-E824-45E5-BE05-A528BAF54E0B', scene_id=4575212)
        # data_provider.load_scene_data(session_uid='F7FBF132-60A6-4B74-A25A-F9A3C19327EE', scene_id=4840195)
        common = Common(data_provider)
        case_counter_calculator = CaseCountCalculator(data_provider, common)
        case_counter_calculator.main_case_count_calculations()
        common.commit_results_data()

