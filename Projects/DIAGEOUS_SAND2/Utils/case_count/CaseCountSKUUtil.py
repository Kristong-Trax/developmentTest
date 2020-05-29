import numpy as np
import pandas as pd
import networkx as nx
import math
from Trax.Algo.Calculations.Core.AdjacencyGraph.Plots import GraphPlot
from plotly.offline import iplot
from consts import Consts
from collections import Counter
from Trax.Cloud.Services.Connector.Logger import Log
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts as Src
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from Trax.Algo.Calculations.Core.AdjacencyGraph.Builders import AdjacencyGraphBuilder
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts as Mc, ProductsConsts as Pc, ScifConsts as Sc


class CaseCountCalculator(GlobalSessionToolBox):
    """This class calculates the Case Count SKU KPI set.
    It uses display tags and sub-products in order to calculate results per target and sum
    all of them in order to calculate the main Case Count"""

    def __init__(self, data_provider, common):
        GlobalSessionToolBox.__init__(self, data_provider, None)
        self.filtered_mdis = self._get_filtered_match_display_in_scene()
        self.store_number_1 = self.store_info.store_number_1[0]
        self.filtered_scif = self._get_filtered_scif()
        self.ps_data_provider = PsDataProvider(data_provider)
        self.target = self._get_case_count_targets()
        self.matches = self.get_filtered_matches()
        self.common = common

    def _get_case_count_targets(self):
        """
        This method fetches the relevant targets for the case count
        """
        case_count_kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.TOTAL_CASES_STORE_KPI)
        targets = self.ps_data_provider.get_kpi_external_targets(kpi_fks=[case_count_kpi_fk], data_fields=[Src.TARGET],
                                                                 key_fields=[Sc.PRODUCT_FK, 'store_number_1'])
        targets = targets.loc[targets.store_number_1 == self.store_number_1][[Pc.PRODUCT_FK, Src.TARGET]]
        return dict(zip(targets[Pc.PRODUCT_FK], targets[Src.TARGET]))

    def _get_filtered_match_display_in_scene(self):
        """ This method filters match display in scene - it saves only "close" and "open" tags"""
        mdis = self.data_provider.match_display_in_scene.loc[
            self.data_provider.match_display_in_scene.display_name.str.contains(Consts.RELEVANT_DISPLAYS_SUFFIX)]
        return mdis

    def main_case_count_calculations(self):
        """This method calculates the entire Case Count KPIs set."""
        if not (self.filtered_mdis.empty or self.filtered_scif.empty or not self.target or self.matches.empty):
            try:
                self._prepare_data_for_calculation()
                facings_res = self._calculate_display_size_facings()
                sku_cases_res = self._count_number_of_cases()
                unshoppable_cases_res = self._non_shoppable_case_kpi()
                implied_cases_res = self._implied_shoppable_cases_kpi()
                total_res = self._calculate_total_cases(sku_cases_res + implied_cases_res + unshoppable_cases_res)
                self._save_results_to_db(facings_res+sku_cases_res+unshoppable_cases_res+implied_cases_res+total_res)
                self._calculate_total_score_level_res(total_res)
            except Exception as err:
                Log.error("DiageoUS Case Count calculation failed due to the following error: {}".format(err))

    def _calculate_total_score_level_res(self, total_res_sku_level_results):
        """This method gets the Total Cases SKU level results and aggregates them in order to create the
        mobile store level result"""
        result, kpi_fk = 0, self.get_kpi_fk_by_kpi_type(Consts.TOTAL_CASES_STORE_KPI)
        for res in total_res_sku_level_results:
            result += res.get(Src.RESULT, 0)
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=int(self.manufacturer_fk), result=result,
                                       denominator_id=self.store_id, identifier_result=kpi_fk)

    def _calculate_total_cases(self, kpi_results):
        """ This method sums # of cases per brand and Implied Shoppable Cases KPIs
        and saves the main result to the DB"""
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.TOTAL_CASES_SKU_KPI)
        total_results_per_sku, results_list = Counter(), list()
        for res in kpi_results:
            total_results_per_sku[res[Pc.PRODUCT_FK]] += res[Src.RESULT]
        for product_fk, target in self.target.iteritems():
            result = total_results_per_sku.get(product_fk, 0)
            results_list.append({Pc.PRODUCT_FK: product_fk, Src.RESULT: result, 'fk': kpi_fk, Src.TARGET: target})
        return results_list

    def _save_results_to_db(self, results_list):
        """This method saves the KPI results to DB"""
        total_cases_sku_fk = int(self.get_kpi_fk_by_kpi_type(Consts.TOTAL_CASES_SKU_KPI))
        total_cases_store_fk = int(self.get_kpi_fk_by_kpi_type(Consts.TOTAL_CASES_STORE_KPI))
        for res in results_list:
            kpi_fk = int(res.get('fk'))
            parent_id = '{}_{}'.format(int(res[Pc.PRODUCT_FK]),
                                       total_cases_sku_fk) if kpi_fk != total_cases_sku_fk else total_cases_store_fk
            kpi_id = '{}_{}'.format(int(res[Pc.PRODUCT_FK]), kpi_fk)
            result, target = res.get(Src.RESULT), res.get(Src.TARGET)
            score = 1 if target is not None and result >= target else 0
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=res[Pc.PRODUCT_FK], result=result, score=score,
                                           target=target, identifier_result=kpi_id, identifier_parent=parent_id,
                                           should_enter=True)

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
        closest_display_data = matches.apply(lambda row: self._apply_closet_point_logic_on_row(row, mdis, 'pk'), axis=1)
        closest_display_data = pd.DataFrame(closest_display_data.values.tolist())
        return closest_display_data

    def _calculate_display_size_facings(self):
        """This method calculates the number of facings for SKUs with the relevant SKU Type
        only in scenes that have displays"""
        filtered_scif = self.filtered_scif.loc[(self.filtered_scif[Sc.SKU_TYPE].isin(Consts.FACINGS_SKU_TYPES))]
        filtered_scif = filtered_scif.loc[filtered_scif[Sc.TAGGED] > 0][[Pc.SUBSTITUTION_PRODUCT_FK, Sc.TAGGED]]
        results_df = filtered_scif.groupby(Pc.SUBSTITUTION_PRODUCT_FK, as_index=False).sum().rename(
            {Sc.TAGGED: Src.RESULT, Sc.SUBSTITUTION_PRODUCT_FK: Sc.PRODUCT_FK}, axis=1)
        results_df = results_df.merge(pd.DataFrame({Pc.PRODUCT_FK: self.target.keys()}), how='right', on=Pc.PRODUCT_FK)
        results_df = results_df.assign(fk=self.get_kpi_fk_by_kpi_type(Consts.FACINGS_KPI))
        results_df = results_df.fillna(0)
        return results_df.to_dict('records')

    def _count_number_of_cases(self):
        """This method counts the number of cases per SKU.
        It identify the closest SKU tag to every case and using it to define the case's brand
        """
        total_res, results_for_db, kpi_fk = Counter(), list(), self.get_kpi_fk_by_kpi_type(Consts.SHOPPABLE_CASES_KPI)
        results = self.matches.groupby(Consts.DISPLAY_IN_SCENE_FK, as_index=False)[Sc.SUBSTITUTION_PRODUCT_FK].apply(
            list).values
        for res in results:
            for sku in res:
                total_res[int(sku)] += (1/float(len(res)))
        for product_fk in self.target.keys():
            result = round(total_res.get(product_fk, 0), 2)
            results_for_db.append({Sc.PRODUCT_FK: product_fk, Src.RESULT: result, 'fk': kpi_fk})
        return results_for_db

    def _implied_shoppable_cases_kpi(self):
        """
        This method calculates the implied shoppable cases KPI. It creates Adjacency graph per scene,
        iterates paths over the cases pile and count the number of hidden cases.
        """
        results = []
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.IMPLIED_SHOPPABLE_CASES_KPI)
        scenes_to_calculate = self.matches.scene_fk.unique().tolist()
        total_score_per_sku = Counter()
        for scene_fk in scenes_to_calculate:
            adj_g = self._create_adjacency_graph_per_scene(scene_fk)
            paths = self._get_relevant_path_for_calculation(adj_g)
            total_score_per_sku += self._calculate_case_count(adj_g, paths)
            self.create_graph_image(scene_fk, adj_g)
        for product_fk in self.target.keys():
            result = total_score_per_sku.get(product_fk, 0)
            # convert results ending in .99 to .00 - this unpleasant situation is caused by using float arithmetic
            if str(result * 100)[-2:] in ['99', '49']:
                result = math.ceil(result * 10) / 10
            results.append({Sc.PRODUCT_FK: product_fk, Src.RESULT: result, 'fk': kpi_fk})
        return results

    def _non_shoppable_case_kpi(self):
        """ This method calculates the number of unshoppable SKUs.
        SKU is considered non shoppable if there are only 'case' SKU Type without any facings' relevant one.
        Please note that if SKU is unshoppable it will get a result of 100!!"""
        unshoppable_results = []
        kpi_fk = self.get_kpi_fk_by_kpi_type(Consts.NON_SHOPPABLE_CASES_KPI)
        grouped_scif = self.filtered_scif.groupby([Sc.SUBSTITUTION_PRODUCT_FK, Sc.SKU_TYPE], as_index=False)[
            Sc.TAGGED].sum().rename({Sc.SUBSTITUTION_PRODUCT_FK: Sc.PRODUCT_FK}, axis=1)
        grouped_scif = grouped_scif.loc[grouped_scif[Sc.TAGGED] > 0]
        grouped_scif_dict = grouped_scif.groupby(Pc.PRODUCT_FK)[Sc.SKU_TYPE].apply(list).to_dict()
        for product_fk in self.target.keys():
            sku_types = grouped_scif_dict.get(product_fk, [])
            res = 1 if len(sku_types) == 1 and sku_types[0].lower() == 'case' else 0
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
            min_distance = min(distances)
        except ValueError:
            Log.error('Unable to find a matching opposite point for supplied anchor!')
            return other_points_df
        return other_points_df[(other_points_df['rect_x'] == closest_point[0]) & (
                other_points_df['rect_y'] == closest_point[1])], min_distance

    def _apply_closet_point_logic_on_row(self, row, mdis, value_to_return='pk'):
        """
        This method gets a Series and filtered Match Display In Scene DataFrame and returns the closet point
        to the point that can be found in the row
        """
        current_point = (row['rect_x'], row['rect_y'])
        relevant_mdis = mdis.loc[(mdis.scene_fk == row[Sc.SCENE_FK])]
        closet_point, min_distance = self.get_closest_point(current_point, relevant_mdis)
        if not closet_point.empty:
            return {Consts.DISPLAY_IN_SCENE_FK: closet_point.iloc[0][value_to_return], Consts.MIN_DIST: min_distance,
                    Mc.SCENE_MATCH_FK: row[Mc.SCENE_MATCH_FK]}
        return {}

    def _add_matches_display_data(self, closest_tag_to_display_df):
        """
        This method calculates the closets match_display_in_scene tag per row and adds
        it to Match Product in Scene DataFrame
        """
        mdis = self.filtered_mdis[[Consts.DISPLAY_IN_SCENE_FK, 'rect_x', 'rect_y', 'display_name', Consts.DISPLAY_SKU]]
        mdis = mdis.merge(closest_tag_to_display_df, how='right', on=Consts.DISPLAY_IN_SCENE_FK)
        mdis.rename({'rect_x': 'display_rect_x', 'rect_y': 'display_rect_y'}, inplace=True, axis=1)
        self.matches = self.matches.merge(mdis, on=Mc.SCENE_MATCH_FK, how='left')

    def _add_displays_the_closet_product_fk(self, closest_tag_to_display_df):
        """
        This method calculates the closets match_display_in_scene tag per row and adds
        it to Match Product in Scene DataFrame
        """
        temp_matches = self.matches[[Mc.SCENE_MATCH_FK, 'substitution_product_fk']]
        display_with_closest_sku = closest_tag_to_display_df.merge(temp_matches, how='left', on=Mc.SCENE_MATCH_FK)
        display_with_closest_sku.rename({'substitution_product_fk': Consts.DISPLAY_SKU}, axis=1, inplace=True)
        display_with_closest_sku = display_with_closest_sku.sort_values(
            [Consts.DISPLAY_IN_SCENE_FK, Consts.MIN_DIST]).drop_duplicates(Consts.DISPLAY_IN_SCENE_FK)
        self.filtered_mdis = self.filtered_mdis.merge(
            display_with_closest_sku[[Consts.DISPLAY_IN_SCENE_FK, Consts.DISPLAY_SKU]],
            right_on=Consts.DISPLAY_IN_SCENE_FK, left_on='pk', how='left')

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
        if not filtered_matches.empty:
            maskings = AdjacencyGraphBuilder._load_maskings(self.project_name, scene_fk)
            add_node_attr = [Consts.DISPLAY_IN_SCENE_FK, 'display_rect_x', 'display_rect_y', 'display_name']
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
                if not (case_status or open_detection_indicator):
                    continue  # Closed case
                else:
                    if open_detection_indicator:
                        case_count_score += self._divide_score_among_skus(adj_g.nodes[node], stacking_layer)
                    else:
                        open_detection_indicator = True
        return case_count_score

    def _divide_score_among_skus(self, node, stacking_layer):
        """In the new Case Count SKU logic, the score is being divided among the SKUs the close to the display.
        So if there 2nd open case in in the 5th layer and it has 3 different SKUs on top of it, each of them will get
        1.67 points"""
        results = Counter()
        match_product_in_scene_fks = node.get('match_fk')
        relevant_skus = self.matches.loc[self.matches.scene_match_fk.isin(match_product_in_scene_fks)][
            Sc.SUBSTITUTION_PRODUCT_FK].values
        for sku in relevant_skus:
            results[sku] += round(stacking_layer / float(len(relevant_skus)), 2)
        return results

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
        if not scif.empty:
            scif[Sc.SUBSTITUTION_PRODUCT_FK] = scif.apply(
                lambda row: int(row[Sc.PRODUCT_FK]) if str(row[Sc.SUBSTITUTION_PRODUCT_FK]) in ['nan', 'None', ''] else
                int(row[Sc.SUBSTITUTION_PRODUCT_FK]), axis=1)
        return scif

    def _get_scenes_with_relevant_displays(self):
        """This method returns only scene with "Open" or "Close" display tags"""
        return self.filtered_mdis.scene_fk.unique().tolist()

    def create_graph_image(self, scene_id, graph=None):
        if not graph:
            return

        filtered_figure = GraphPlot.plot_networkx_graph(graph, overlay_image=True,
                                                        scene_id=scene_id, project_name='diageous-sand2')
        filtered_figure.update_layout(autosize=False, width=1000, height=800)
        iplot(filtered_figure)


if __name__ == '__main__':
    from KPIUtils_v2.DB.CommonV2 import Common
    from Trax.Utils.Conf.Configuration import Config
    from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
    Config.init('')
    test_data_provider = KEngineDataProvider('diageous-sand2')
    sessions = ['b0cb6544-2609-473d-ac91-6e280c61eaff']
    for session in sessions:
        print(session)
        test_data_provider.load_session_data(session_uid=session)
        test_common = Common(test_data_provider)
        case_counter_calculator = CaseCountCalculator(test_data_provider, test_common)
        case_counter_calculator.main_case_count_calculations()
        test_common.commit_results_data()
