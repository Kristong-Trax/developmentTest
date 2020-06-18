from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
import math


class NumberOfUniqueSKUsKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueSKUsKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_SKUS_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_UNQIUE_SKUS_KPI]
        fields_df = template[[Consts.FIELD, Consts.TARGET_MIN, Consts.TARGET_MAX]]
        if template.empty:
            categories = ['Core Salty']
        else:
            categories = template.iloc[0][Consts.CATEGORY].split(",")
        sku_results = self.dependencies_data
        df = self.utils.match_product_in_scene_wo_hangers.copy()
        df['facings'] = 1
        store_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        # filter only specific categories
        df = df[(df['category'].isin(categories)) & (df['manufacturer_fk'] == self.utils.own_manuf_fk)]
        category_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
            ['bay_number', 'shelf_number', 'facings']]
        category_df.columns = ['bay_number', 'shelf_number', 'facings category']
        join_df = store_df.merge(category_df, on=['bay_number', 'shelf_number'], how="left").fillna(0)
        join_df['percentage'] = join_df['facings category'] / join_df['facings']
        # number of shelves with more than 50% strauss products
        denominator = len(join_df)
        numerator = len(join_df[join_df['percentage'] >= 0.5])
        number_of_unique_skus = len(sku_results)
        sadot = math.ceil(numerator / 5.0)
        sadot = sadot if sadot != 0 else 1
        target, upper_target = self.get_target(fields_df, sadot)
        if target == -1:
            score = Consts.NO_TARGET
            ratio = 0
        else:
            score = Consts.PASS if target <= number_of_unique_skus <= upper_target else Consts.FAIL
            ratio = (number_of_unique_skus / float(upper_target)) * 100
        self.write_to_db_result(fk=kpi_fk, numerator_id=self.utils.own_manuf_fk, denominator_id=self.utils.store_id,
                                numerator_result=number_of_unique_skus, denominator_result=denominator, result=ratio,
                                target=target, weight=sadot, score=score)

    @staticmethod
    def get_target(fields_df, sadot):
        if sadot in fields_df[Consts.FIELD].values:
            target = fields_df[fields_df['Field'] == sadot][Consts.TARGET_MIN].values[0]
            upper_target = fields_df[fields_df['Field'] == sadot][Consts.TARGET_MAX].values[0]
        elif sadot >= fields_df[Consts.FIELD].max():
            target = fields_df[fields_df['Field'] == fields_df[Consts.FIELD].max()][Consts.TARGET_MIN].values[0]
            upper_target = fields_df[fields_df['Field'] == fields_df[Consts.FIELD].max()][Consts.TARGET_MAX].values[0]
        else:
            target = upper_target = -1
        return target, upper_target

    def kpi_type(self):
        pass

