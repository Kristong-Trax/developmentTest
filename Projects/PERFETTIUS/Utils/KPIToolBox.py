
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
# import pandas as pd

from Projects.PERFETTIUS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AdjacencyCalculations_v2 import Adjancency

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'huntery'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.adjacency = Adjancency(data_provider)
        self.block = Block(data_provider)

    def main_calculation(self):
        score = 0
        return score

    def calculate_presence(self):
        pass

    def calculate_shelf_location(self):
        pass

    def calculate_blocking(self, kpi_name, template_fk=None):
        kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
        location = {'template_fk': template_fk} if template_fk else None
        population = {'brand_name': 'Mentos'}
        blocks = self.block.network_x_block_together(population, location)


    def calculate_adjacency(self, kpi_name, primary_product_filters, secondary_product_filters, template_fk=None):
        kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
        location = {'template_fk': template_fk} if template_fk else None
        population = {
            'anchor_products': {'brand_name': ['Mentos']},
            'tested_products': {'brand_name': ['Ice Breakers', 'Tic Tac']}
        }
        adj_df = self.adjacency.network_x_adjacency_calculation(population, location)
        result = 1 if not adj_df[adj_df['is_adj'] is True].empty else 0
        self.write_to_db(kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=template_fk, result=result)



