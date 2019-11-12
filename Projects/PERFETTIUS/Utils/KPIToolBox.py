
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

        products_list = products_df['product_fk'].unique().tolist()

        if shelf_location == 'top':
            shelf_matches = relevant_matches[(relevant_matches['product_fk'].isin(products_list)) &
                                             (relevant_matches['shelf_number'] <= (shelves / 3))]
        elif shelf_location == 'middle_bottom':
            shelf_matches = relevant_matches[(relevant_matches['product_fk'].isin(products_list)) &
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
        blocks = self.block.network_x_block_together({config.numerator_param: config.numerator_value}, location,
                                                     additional={'check_vertical_horizontal': True})
        if not blocks.empty:
            blocks = blocks[blocks['is_block']]
            orientation = config.orientation
            if orientation and orientation is not pd.np.nan:
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
        anchor_pks = \
            self.scif[self.scif[config.anchor_param].isin(config.anchor_value)]['product_fk'].unique().tolist()
        tested_pks = \
            self.scif[self.scif[config.tested_param].isin(config.tested_value)]['product_fk'].unique().tolist()
        # handle populations that are not mutually exclusive
        tested_pks = [x for x in tested_pks if x not in anchor_pks]

        population = {'anchor_products': {'product_fk': anchor_pks},
                      'tested_products': {'product_fk': tested_pks}
                      }

        # this function is only needed until the adjacency function is enhanced to not crash when an empty population
        # is provided
        if self.check_population_exists(population, template_fk):
            try:
                adj_df = self.adjacency.network_x_adjacency_calculation(population, location,
                                                                        {'minimum_facings_adjacent': 1,
                                                                         'minimum_block_ratio': 0,
                                                                         'minimum_facing_for_block': 1,
                                                                         'include_stacking': True})
            except AttributeError:
                Log.info("Error calculating adjacency for kpi_fk {} template_fk {}".format(kpi_fk, template_fk))
                return
            if adj_df.empty:
                result = 0
            else:
                result = 1 if not adj_df[adj_df['is_adj']].empty else 0
        else:
            result = 0
        numerator_id = self.get_brand_fk_from_brand_name(config.anchor_value[0])
        self.write_to_db(kpi_fk, numerator_id=numerator_id, denominator_id=template_fk, result=result)
        return

    @run_for_every_scene_type
    def calculate_brand_facings(self, kpi_fk, template_fk=None):
        relevant_scif = self.scif[self.scif['template_fk'] == template_fk]

        denominator_results = relevant_scif.groupby('Customer Category', as_index=False)[
            ['facings']].sum().rename(columns={'facings': 'denominator_result'})

        numerator_result = relevant_scif.groupby(['brand_fk', 'Customer Category'], as_index=False)[
            ['facings']].sum().rename(columns={'facings': 'numerator_result'})

        results = numerator_result.merge(denominator_results)
        results['result'] = (results['numerator_result'] / results['denominator_result'])
        results['result'].fillna(0, inplace=True)

        for index, row in results.iterrows():
            relevant_perfetti_product_fk = self.get_product_fk_from_perfetti_category(row['Customer Category'])
            self.write_to_db(fk=kpi_fk, numerator_id=row['brand_fk'], denominator_id=relevant_perfetti_product_fk,
                             numerator_result=row['numerator_result'], denominator_result=row['denominator_result'],
                             context_id=template_fk, result=row['result'], score=row['result'])

    def get_kpi_function_by_family_fk(self, kpi_family_fk):
        if kpi_family_fk == 19:
            return self.calculate_presence
        elif kpi_family_fk == 20:
            return self.calculate_adjacency
        elif kpi_family_fk == 21:
            return self.calculate_blocking
        elif kpi_family_fk == 22:
            return self.calculate_shelf_location
        elif kpi_family_fk == 23:
            return self.calculate_brand_facings

    def get_external_target_data_by_kpi_fk(self, kpi_fk):
        return self.external_targets[self.external_targets['kpi_fk'] == kpi_fk].iloc[0]

    def get_brand_fk_from_brand_name(self, brand_name):
        return self.all_products[self.all_products['brand_name'] == brand_name]['brand_fk'].iloc[0]

    def get_product_fk_from_perfetti_category(self, perfetti_category):
        try:
            return self.all_products[self.all_products['Customer Category'] == perfetti_category]['product_fk'].iloc[0]
        except IndexError:
            return None

    def check_population_exists(self, population, template_fk):
        relevant_scif = self.scif[self.scif['template_fk'] == template_fk]
        anchor_scif = relevant_scif[relevant_scif['product_fk'].isin(population['anchor_products']['product_fk'])]
        tested_scif = relevant_scif[relevant_scif['product_fk'].isin(population['tested_products']['product_fk'])]
        if anchor_scif.empty or tested_scif.empty:
            return False
        else:
            return True



