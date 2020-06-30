from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class SosVsTargetSubBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetSubBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.SUB_BRAND_SOS)
        self.calculate_sub_brand_out_of_category_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_sub_brand_out_of_category_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.SUB_BRAND_SOS)
        filtered_scif = self.util.filtered_scif
        category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                            as_index=False).agg({'updated_gross_length': np.sum})
        category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
        sub_brand_cat_df = filtered_scif.groupby(["sub_brand", ScifConsts.CATEGORY_FK],
                                             as_index=False).agg({'updated_gross_length': np.sum})
        sub_brand_cat_df = sub_brand_cat_df.merge(category_df, on=ScifConsts.CATEGORY_FK, how='left')
        sub_brand_cat_df['sos'] = sub_brand_cat_df['updated_gross_length'] / sub_brand_cat_df['cat_len']
        sub_brand_cust_entity = self.util.custom_entities[self.util.custom_entities['entity_type'] == 'sub_brand']
        sub_brand_cat_df = sub_brand_cat_df.merge(sub_brand_cust_entity, left_on= 'sub_brand', right_on='name',
                                                  how='left')
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        for i, row in sub_brand_cat_df.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row["pk"],
                                    numerator_result=row['updated_gross_length'],
                                    denominator_id=row[ScifConsts.CATEGORY_FK],
                                    denominator_result=row['cat_len'], result=row['sos'] * 100,
                                    context_id=location_type_fk)
            self.util.add_kpi_result_to_kpi_results_df(
                [kpi_fk, row["sub_brand"], row[ScifConsts.CATEGORY_FK], row['sos'] * 100, None, None])
