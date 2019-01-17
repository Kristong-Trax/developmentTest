
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.GraphicalModel.AdjacencyGraphs import AdjacencyGraph

# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class GPUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """


        score = 0
        return score

    def adjacency(self, kpi_set_fk, kpi_name):
        relevant_scif = self.filter_df(self.scif.copy(), {'template_name': 'Shelf'})
        template = self.Adjaceny_template.loc[self.Adjaceny_template['KPI'] == kpi_name]
        kpi_template = template.loc[template['KPI'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        Param = kpi_template['param']
        Value1 = str(kpi_template['Product Att']).replace(', ', ',').split(',')
        filter = {Param: Value1}

        for scene in relevant_scif.scene_fk.unique():
            scene_filter = {'scene_fk': scene}
            mpis = self.filter_df(self.mpis, scene_filter)
            allowed = {}
            items = set(self.filter_df(mpis, filter)['scene_match_fk'].values)
            # allowed_items = set(self.filter_df(mpis, allowed)['scene_match_fk'].values)
            # items.update(allowed_items)
            if not (items):
                return

            all_graph = AdjacencyGraph(mpis, None, self.products,
                                       product_attributes=['rect_x', 'rect_y'],
                                       name=None, adjacency_overlap_ratio=.4)

            match_to_node = {int(node['match_fk']): i for i, node in all_graph.base_adjacency_graph.nodes(data=True)}
            node_to_match = {val: key for key, val in match_to_node.items()}
            edge_matches = set(
                sum([[node_to_match[i] for i in all_graph.base_adjacency_graph[match_to_node[item]].keys()]
                     for item in items], []))
            adjacent_items = edge_matches - items
            adj_mpis = mpis[(mpis['scene_match_fk'].isin(adjacent_items))]

            #Check left and right product of adj_mpis
            #numerator = product
            #denominator = category next to product
            #score = left(0) or right(1)

            numerator_id = \
            self.all_products['sub_category_fk'][self.all_products['sub_category'] == Value1[0]].iloc[0]

            denominator_id = self.all_products['sub_category_fk'][
                self.all_products['sub_category'] == adjacent_sub_category].iloc[0]

            product_sequence = product['sequence']
            for adj_item in adj_mpis:
                adj_mpi_sequence = adj_item['sequence_number']
                if (adj_mpi_sequence) ==  (product_seqeunce +1):

                    self.common.write_to_db_result(fk=kpi_set_fk, numerator_id=numerator_id,
                                               denominator_id=denominator_id, result=1, score=1)




    def shareofshelf(self):
        def calculate_main_kpi(self, main_line):
            kpi_name = main_line[Const.KPI_NAME]
            kpi_type = main_line[Const.Type]
            template_groups = self.does_exist(main_line, Const.TEMPLATE_GROUP)

            general_filters = {}

            scif_template_groups = self.scif['template_group'].unique().tolist()
            # encoding_fixed_list = [template_group.replace("\u2013","-") for template_group in scif_template_groups]
            # scif_template_groups = encoding_fixed_list

            store_type = self.store_info["store_type"].iloc[0]
            store_types = self.does_exist_store(main_line, Const.STORE_TYPES)
            if store_type in store_types:

                if template_groups:
                    if ('All' in template_groups) or bool(set(scif_template_groups) & set(template_groups)):
                        if not ('All' in template_groups):
                            general_filters['template_group'] = template_groups
                        if kpi_type == Const.SOVI:
                            relevant_template = self.templates[kpi_type]
                            relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]

                            if relevant_template["numerator param 1"].all() and relevant_template[
                                "denominator param"].all():
                                function = self.get_kpi_function(kpi_type)
                                for i, kpi_line in relevant_template.iterrows():
                                    result, score = function(kpi_line, general_filters)
                        else:
                            pass

                else:

                    pass


    def calculate_category_space(self, kpi_set_fk, kpi_name, category, scene_types=None):
        template = self.all_template_data.loc[(self.all_template_data['KPI Level 2 Name'] == kpi_name) &
                                              (self.all_template_data['Value1'] == category)]
        kpi_template = template.loc[template['KPI Level 2 Name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []
        category_att = 'category'

        filters = {'template_name': scene_types, 'category': kpi_template['Value1']}

        # if kpi_template['Value1'] in CATEGORIES:
        #     category_att = 'category'

        if kpi_template['Value1']:
            values_to_check = self.all_products.loc[self.all_products[category_att] == kpi_template['Value1']][
                category_att].unique().tolist()

        for primary_filter in values_to_check:
            filters[kpi_template['Param1']] = primary_filter

            new_kpi_name = self.kpi_name_builder(kpi_name, **filters)

            result = self.calculate_category_space_length(new_kpi_name,
                                                          **filters)
            filters['Category'] = kpi_template['KPI Level 2 Name']
            score = result
            numerator_id = self.products['category_fk'][self.products['category'] == kpi_template['Value1']].iloc[0]
            self.common.write_to_db_result_new_tables(kpi_set_fk, numerator_id, 999, score, score=score)

    def calculate_category_space_length(self, kpi_name, threshold=0.5, retailer=None, exclude_pl=False, **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            space_length = 0
            bay_values = []
            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.mpis[self.mpis['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay


                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()

                    if tested_group_linear_value:
                        bay_ratio = tested_group_linear_value / float(bay_total_linear)
                    else:
                        bay_ratio = 0

                    if bay_ratio >= threshold:
                        category = filters['category']
                        max_facing = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                       (scene_matches['stacking_layer'] == 1)][
                            'facing_sequence_number'].max()
                        shelf_length = self.spacing_template_data.query('Category == "' + category +
                                                                        '" & Low <= "' + str(
                            max_facing) + '" & High >= "' + str(max_facing) + '"')
                        shelf_length = int(shelf_length['Size'].iloc[-1])
                        bay_values.append(shelf_length)
                        space_length += shelf_length
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length
