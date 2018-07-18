import os
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Calculations.AdjacencyCalculations import Adjancency
from Projects.CUBAU.Utils.ParseTemplates import parse_template
from KPIUtils_v2.Calculations.BlockCalculations import Block
from Trax.Algo.Calculations.Core.DataProvider import Data

BLOCK = 'Block'
ADJACENCY = 'Adjacency'

class Summary:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.adjancency = Adjancency(self.data_provider)
        self.block = Block(self.data_provider)
        self.template_name = 'summary_kpis.xlsx'
        self.TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data', self.template_name)
        self.template_data = parse_template(self.TEMPLATE_PATH, "KPIs")


    # @log_runtime('Main Calculation')
    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_adjacency()
        self.calculate_block()
        return

    def calculate_adjacency(self):
        # self.adjancency.calculate_adjacency()
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == BLOCK]
        # for index, row in kpi_data.iterrows():
        return

    def calculate_block(self):
        kpi_data = self.template_data.loc[self.template_data['KPI Type'] == BLOCK]
        for index, row in kpi_data.iterrows():
            filters = {}
            entity_type = row['Group A Entity Type']
            entity_value = row['Group A Entity Value'].strip().split(",")
            filters[entity_type] = entity_value
            filters['display_name'] = row['template_display name'].strip().split(",")
            block = self.block.calculate_block_together(minimum_block_ratio=1, filters=filters)
        return
