from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log
import math


class NumberOfUniqueSKUsKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueSKUsKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_SKUS_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_UNQIUE_SKUS_KPI]
        number_of_fields = template[['Field', 'Target']]
        if template.empty:
            categories = ['Core Salty']
        else:
            categories = template['category'][0].split(",")
        sku_results = self.dependencies_data
        df = self.utils.match_product_in_scene_wo_hangers.copy()
        df['facings'] = 1
        store_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        # filter only specific categories
        df = df[df['category_fk'].isin(categories)]
        category_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        shelves_category_percentage = category_df / store_df
        denominator = len(shelves_category_percentage)
        # number of shelves with more than 50% strauss products
        numerator = len(shelves_category_percentage[shelves_category_percentage['facings'] >= 0.5])

        # number of strauss facings on all shelves
        facings = sku_results['result'].sum()

        # Adding 0.001 to prevent 0 sadot case
        sadot = math.ceil((numerator + 0.001) / 5.0)
        relevant_field = number_of_fields[number_of_fields['Field'] == sadot]
        if not relevant_field.empty:
            target = relevant_field['Target']
        else:
            target = -1
        # todo: no target case
        score = 1 if facings >= target else 2
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk, denominator_id=self.utils.store_id,
                                numerator_result=numerator, denominator_result=denominator, result=facings,
                                target=target, weight=sadot, score=score)

    def kpi_type(self):
        pass

