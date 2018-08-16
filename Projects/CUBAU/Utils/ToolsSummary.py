import os
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from Projects.CUBAU.Utils.ParseTemplates import parse_template
from Projects.CUBAU.Utils.GeneralToolBox import CUBAUCUBAUGENERALToolBox
from KPIUtils_v2.Calculations.BlockCalculations import Block
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils.DB.Common import Common

BLOCK = 'Block'
ADJACENCY = 'Adjacency'
COMPETITOR_ADJACENCY = 'Competitor Adjacency'
KPI_SET = 'Summary KPIs'

class Summary:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.adjacency = Adjancency(self.data_provider)
        self.block = Block(self.data_provider)
        self.template_name = 'summary_kpis.xlsx'
        self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data', self.template_name)
        self.template_data = parse_template(self.TEMPLATE_PATH, "KPIs")
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.tools = GENERALToolBox(self.data_provider)
        self.common = Common(self.data_provider)
        self.kpi_results_queries = []
        self.cub_tools = CUBAUCUBAUGENERALToolBox(self.data_provider, self.output)
        self.store_type = self.data_provider.store_type
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.session_uid= self.data_provider.session_uid
        self.visit_date = self.data_provider.visit_date

    # @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calc_adjacency()
        self.calc_block()
        self.calc_competitor_adjacency()
        return

    def calc_adjacency(self):
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == ADJACENCY]
        for index, row in kpi_data.iterrows():
            # kpi_filters = {}
            # kpi_filters['template_display_name'] = row['display name'].split(",")
            adjacent_products = self.calculate_relative_adjacency(row)
            if adjacent_products:
                self.write_to_db(row, 100, adjacent_products)
            else:
                self.write_to_db(row, 0, adjacent_products)

        return

    def calc_competitor_adjacency(self):
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == COMPETITOR_ADJACENCY]
        for index, row in kpi_data.iterrows():
            kpi_filters = {}
            kpi_filters['template_display_name'] = row['display name'].split(",")
            if row['Store additional attribute 1']:
                if not self.store_info['additional_attribute_1'].values[0] in row['Store additional attribute 1'].split(","):
                    continue
            competitor_products = self.calculate_relative_adjacency(row, **kpi_filters)
            if competitor_products:
                self.write_to_db(row, 100, 1)
            else:
                self.write_to_db(row, 0, 0)
        return

    def calc_block(self):
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == BLOCK]
        for index, row in kpi_data.iterrows():
            result = None
            kpi_filters = {}
            if row['store type']:
                if not row['store type'] == self.store_type:
                    continue
            entity_type = row['Block Entity Type']
            entity_value = row['Block Entity Value'].split(",")
            kpi_filters[entity_type] = entity_value
            kpi_filters['template_display_name'] = row['display name'].strip().split(",")
            threshold = row['Target']
            if threshold:
                threshold = float(threshold)
            block_result = self.cub_tools.calculate_block_together(minimum_block_ratio=threshold, **kpi_filters)
            # block_result = self.block.calculate_block_together(minimum_block_ratio=threshold, **kpi_filters) #todo:open bug to Ilan
            filters, relevant_scenes = self.cub_tools.separate_location_filters_from_product_filters(**kpi_filters)
            score = 100 if block_result else 0
            if len(relevant_scenes) == 0:
                result = 'No relevant products or scenes'
                score = None
            self.write_to_db(row, score, result)
        return

    def calculate_relative_adjacency(self, kpi_data, **general_filters):
        adjacencies = []
        if kpi_data.empty:
            return None
        direction_data = {'top': (1, 1), 'bottom': (1, 1), 'left': (1, 1), 'right': (1, 1)}
        entity_type = kpi_data['Block Entity Type']
        entity_value = kpi_data['Block Entity Value'].split(",")
        tested_filters = {entity_type: entity_value}
        entity_to_return = kpi_data['Anchor Entity to return']
        if kpi_data['Anchor Entity Type'] != '':
            anchor_filters = {kpi_data['Anchor Entity Type']:kpi_data['Anchor Entity Value']}
        else:
            anchor_filters = None
        if not self.scif.empty:
            adjacencies = self.cub_tools.calculate_adjacency_relativeness(tested_filters, direction_data, entity_to_return, anchor_filters,
                                                                           **general_filters)
        if adjacencies:
            total_result = 0
            for neighbor in list(set(adjacencies)):
                if str(neighbor) != 'nan':
                    if not total_result:
                        total_result = neighbor.replace("'", "\\'").encode('utf-8')
                    else:
                        total_result = total_result + ', ' + neighbor.replace("'", "\\'").encode('utf-8')
        else:
            total_result = None
        # self.write_to_db(kpi_data, score, total_result)
        return str(total_result)


    def write_to_db(self, kpi_data, score, result=None):
        kpi_name = kpi_data['KPI']
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name, 2)
        # store_fk = self.store_info.store_fk.values[0]
        atomic_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name, 3)
        self.common.write_to_db_result(kpi_fk, 2, score)
        kwargs = self.common.create_basic_dict(atomic_kpi_fk)
        kwargs['display_text'] = kpi_name
        kwargs['result'] = result
        self.common.write_to_db_result(fk=kpi_fk, level=3, score=score, **kwargs)
                                       # session_uid=self.session_uid, display_text=kpi_name, kps_name=KPI_SET,
                                       # atomic_kpi_fk=atomic_kpi_fk, store_fk=store_fk, result=result)
