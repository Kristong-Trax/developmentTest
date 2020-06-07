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
        fields_df = template[['Field', 'Target']]
        if template.empty:
            categories = ['Core Salty']
        else:
            categories = template.iloc[0]['category'].split(",")
        sku_results = self.dependencies_data
        df = self.utils.match_product_in_scene_wo_hangers.copy()
        df['facings'] = 1
        store_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        # filter only specific categories
        df = df[(df['category_fk'].isin(categories)) & (df['manufacturer_fk'] == self.utils.own_manuf_fk)]
        category_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        category_df.columns = ['bay_number', 'shelf_number', 'facings category']
        join_df = store_df.merge(category_df, on=['bay_number', 'shelf_number'], how="left").fillna(0)
        join_df['percentage'] = join_df['facings category'] / join_df['facings']
        # number of shelves with more than 50% strauss products
        denominator = len(join_df)
        numerator = len(join_df[join_df['percentage'] >= 0.5])

        # number of strauss facings on all shelves
        facings = sku_results['result'].sum()

        # Adding 0.001 to prevent 0 sadot case
        sadot = math.ceil((numerator + 0.001) / 5.0)
        relevant_field = fields_df[fields_df['Field'] == sadot]
        if not relevant_field.empty:
            target = relevant_field['Target'].values[0]
        else:
            target = -1
        # todo: no target case
        score = 1 if facings >= target else 2
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk, denominator_id=self.utils.store_id,
                                numerator_result=numerator, denominator_result=denominator, result=facings,
                                target=target, weight=sadot, score=score)

    def kpi_type(self):
        pass

