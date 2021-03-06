from Projects.PEPSICOUK_SAND.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class SosVsTargetBrandKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosVsTargetBrandKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        self.util.filtered_scif, self.util.filtered_matches = \
            self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif,
                                                                                 self.util.filtered_matches,
                                                                                 self.util.BRAND_SOS)
        self.calculate_brand_out_of_category_sos()
        self.util.reset_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_brand_out_of_category_sos(self):
        location_type_fk = self.util.all_templates[self.util.all_templates[ScifConsts.LOCATION_TYPE] == 'Primary Shelf'] \
            [ScifConsts.LOCATION_TYPE_FK].values[0]
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.BRAND_SOS)
        filtered_scif = self.util.filtered_scif
        category_df = filtered_scif.groupby([ScifConsts.CATEGORY_FK],
                                            as_index=False).agg({'updated_gross_length': np.sum})
        category_df.rename(columns={'updated_gross_length': 'cat_len'}, inplace=True)
        brand_cat_df = filtered_scif.groupby([ScifConsts.BRAND_FK, ScifConsts.CATEGORY_FK],
                                             as_index=False).agg({'updated_gross_length': np.sum})
        brand_cat_df = brand_cat_df.merge(category_df, on=ScifConsts.CATEGORY_FK, how='left')
        brand_cat_df['sos'] = brand_cat_df['updated_gross_length'] / brand_cat_df['cat_len']
        for i, row in brand_cat_df.iterrows():
            self.write_to_db_result(fk=kpi_fk, numerator_id=row[ScifConsts.BRAND_FK],
                                    numerator_result=row['updated_gross_length'],
                                    denominator_id=row[ScifConsts.CATEGORY_FK],
                                    denominator_result=row['cat_len'], result=row['sos'] * 100,
                                    context_id=location_type_fk)
            self.util.add_kpi_result_to_kpi_results_df(
                        [kpi_fk, row[ScifConsts.BRAND_FK], row[ScifConsts.CATEGORY_FK], row['sos'] * 100, None,
                         None])


