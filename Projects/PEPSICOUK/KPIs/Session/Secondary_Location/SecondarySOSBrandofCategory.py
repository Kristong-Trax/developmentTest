from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
import pandas as pd
import numpy as np


class SecondarySOSBrandofCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SecondarySOSBrandofCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)
        self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        if self.util.commontools.are_all_bins_tagged:
            self.util.filtered_scif_secondary, self.util.filtered_matches_secondary = \
                self.util.commontools.set_filtered_scif_and_matches_for_specific_kpi(self.util.filtered_scif_secondary,
                                                                                     self.util.filtered_matches_secondary,
                                                                                     self.kpi_name)
            self.calculate_brand_out_of_category_sos()
            self.util.reset_secondary_filtered_scif_and_matches_to_exclusion_all_state()

    def calculate_brand_out_of_category_sos(self):
        kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.kpi_name)
        filtered_scif = self.util.filtered_scif_secondary
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
                                    denominator_result=row['cat_len'], result=row['sos'] * 100)
