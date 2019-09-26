
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import json
import pandas as pd

from Projects.PERFETTIUS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Parsers.ParseInputKPI import filter_df

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


def run_for_every_scene_type(kpi_func):
    def wrapper(*args, **kwargs):
        if args[0].scene_types:
            for template_fk in args[0].scene_types:
                kpi_func(*args, template_fk=template_fk)
        else:
            return kpi_func(*args, **kwargs)
        return
    return wrapper


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.adjacency = Adjancency(data_provider)
        self.block = Block(data_provider)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(data_provider)
        self._scene_types = None
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()

    @property
    def scene_types(self):
        if not self._scene_types:
            self._scene_types = self.scif['template_fk'].unique().tolist()
        return self._scene_types

    def main_calculation(self):
        custom_kpis = self.kpi_static_data[(self.kpi_static_data['kpi_calculation_stage_fk'] == 3) &
                                           (self.kpi_static_data['valid_from'] <= self.visit_date) &
                                           ((self.kpi_static_data['valid_until']).isnull() |
                                            (self.kpi_static_data['valid_until'] >= self.visit_date))]

        for kpi in custom_kpis.itertuples():
            kpi_function = self.get_kpi_function_by_family_fk(kpi.kpi_family_fk)
            kpi_function(kpi.pk)
        return

    @run_for_every_scene_type
    def calculate_presence(self, kpi_fk, template_fk=None):
        config = self.get_external_target_data_by_kpi_fk(kpi_fk)
        if config.empty or (template_fk is None):
            return

        result_df = self.scif[self.scif[config.numerator_param].isin(config.numerator_value) &
                              (self.scif['template_fk'] == template_fk)]
        numerator_id = self.get_brand_fk_from_brand_name(config.numerator_value[0])
        result = 0 if result_df.empty else 1
        self.write_to_db(kpi_fk, numerator_id=numerator_id, denominator_id=template_fk,
                         result=result)
        return

    @run_for_every_scene_type
    def calculate_shelf_location(self, kpi_fk, template_fk=None):
        config = self.get_external_target_data_by_kpi_fk(kpi_fk)
        shelf_location = config.shelf_location
        if config.empty or (template_fk is None):
            return

        relevant_scene_fks = self.scif[self.scif['template_fk'] == template_fk]['scene_fk'].unique().tolist()
        relevant_matches = self.matches[self.matches['scene_fk'].isin(relevant_scene_fks)]

        shelves = relevant_matches.groupby('bay_number', as_index=False)['shelf_number'].max()['shelf_number'].mean()

        products_df = self.scif[(self.scif[config.numerator_param].isin(config.numerator_value)) &
                                (self.scif['template_fk'] == template_fk)]

        if shelf_location == 'top':
            shelf_matches = relevant_matches[(relevant_matches['product_fk'].isin(products_df)) &
                                             (relevant_matches['shelf_number'] <= (shelves / 3))]
        elif shelf_location == 'middle_bottom':
            shelf_matches = relevant_matches[(relevant_matches['product_fk'].isin(products_df)) &
                                             (relevant_matches['shelf_number'] > (shelves / 3))]
        else:
            shelf_matches = pd.DataFrame()

        numerator_id = self.get_brand_fk_from_brand_name(config.numerator_value[0])
        result = 0 if shelf_matches.empty else 1
        self.write_to_db(kpi_fk, numerator_id=numerator_id, denominator_id=template_fk,
                         result=result)

    @run_for_every_scene_type
    def calculate_blocking(self, kpi_fk, template_fk=None):
        config = self.get_external_target_data_by_kpi_fk(kpi_fk)
        if config.empty or (template_fk is None):
            return
        location = {'template_fk': template_fk}
        blocks = self.block.network_x_block_together({config.numerator_param: config.numerator_value}, location)
        if not blocks.empty:
            blocks = blocks[blocks['is_block']]
            orientation = config.orientation
            if orientation:
                blocks = blocks[blocks['orientation'] == orientation]

        numerator_id = self.get_brand_fk_from_brand_name(config.numerator_value[0])
        result = 0 if blocks.empty else 1
        self.write_to_db(kpi_fk, numerator_id=numerator_id, denominator_id=template_fk,
                         result=result)

    @run_for_every_scene_type
    def calculate_adjacency(self, kpi_fk, template_fk=None):
        config = self.get_external_target_data_by_kpi_fk(kpi_fk)
        if config.empty or (template_fk is None):
            return
        location = {'template_fk': template_fk}
        population = {'anchor_products': {config.anchor_param: config.anchor_value},
                      'tested_products': {config.tested_param: config.tested_value}
                      }
        try:
            adj_df = self.adjacency.network_x_adjacency_calculation(population, location, {})
        except AttributeError:
            Log.error("Error calculating adjacency for kpi_fk {} template_fk {}".format(kpi_fk, template_fk))
            return
        if adj_df.empty:
            result = 0
        else:
            result = 1 if not adj_df[adj_df['is_adj']].empty else 0
        numerator_id = self.get_brand_fk_from_brand_name(config.anchor_value[0])
        self.write_to_db(kpi_fk, numerator_id=numerator_id, denominator_id=template_fk, result=result)
        return

    def get_kpi_function_by_family_fk(self, kpi_family_fk):
        if kpi_family_fk == 19:
            return self.calculate_presence
        elif kpi_family_fk == 20:
            return self.calculate_adjacency
        elif kpi_family_fk == 21:
            return self.calculate_blocking
        elif kpi_family_fk == 22:
            return self.calculate_shelf_location

    def get_external_target_data_by_kpi_fk(self, kpi_fk):
        return self.external_targets[self.external_targets['kpi_fk'] == kpi_fk].iloc[0]

    def get_brand_fk_from_brand_name(self, brand_name):
        return self.all_products[self.all_products['brand_name'] == brand_name]['brand_fk'].iloc[0]



