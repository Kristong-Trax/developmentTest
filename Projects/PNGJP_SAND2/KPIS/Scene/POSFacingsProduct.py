from Projects.PNGJP_SAND2.KPIS.Util import PNGJP_SAND2Util
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ScifConsts, ProductsConsts
import numpy as np


class POSFacingsProductKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(POSFacingsProductKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2Util(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.scif.empty:
            template_fk = self.util.scif[ScifConsts.TEMPLATE_FK].values[0]
            kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.POS_FCOUNT_NORMAL)
            matches = self.util.matches_product
            matches = matches[matches[ProductsConsts.PRODUCT_TYPE] == 'POS']
            result_df = matches.groupby([MatchesConsts.PRODUCT_FK], as_index=False).agg({'count': np.sum})
            for i, row in result_df.iterrows():
                self.write_to_db_result(fk=kpi_fk, numerator_id=row[MatchesConsts.PRODUCT_FK],
                                        denominator_id=template_fk,
                                        result=row['count'], by_scene=True)