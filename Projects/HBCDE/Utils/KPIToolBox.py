
from KPIUtils_v2.Utils.Consts.PS import AssortmentProductConsts, AssortmentGroupConsts
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.HBCDE.Data.LocalConsts import Consts
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
# from Trax.Utils.Logging.Logger import Log

__author__ = 'ilays'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.assortment = Assortment(self.data_provider, self.output)

    def main_calculation(self):
        self.calculate_assortment()

    @kpi_runtime()
    def calculate_assortment(self):
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        if lvl3_result.empty:
            return
        kpi_fks = lvl3_result[Consts.KPI_FK_LVL_2].unique()
        for kpi_fk in kpi_fks:
            df = lvl3_result[lvl3_result[Consts.KPI_FK_LVL_2] == kpi_fk]
            sku_kpi_fk = df[Consts.KPI_FK_LVL_3].values[0]
            assortment_group_fk = df[AssortmentGroupConsts.ASSORTMENT_GROUP_FK].values[0]
            assortment_fk = df[AssortmentProductConsts.ASSORTMENT_FK].values[0]
            df.apply(lambda row: self.write_to_db(fk=sku_kpi_fk, numerator_id=row[ProductsConsts.PRODUCT_FK],
                                                  denominator_id=assortment_fk, result=row[Consts.IN_STORE],
                                                  score=row[Consts.IN_STORE], identifier_parent=kpi_fk,
                                                  should_enter=True), axis=1)
            denominator = len(df)
            numerator = len(df[df[Consts.IN_STORE] == 1])
            result = numerator / float(denominator)
            self.write_to_db(fk=kpi_fk, numerator_id=assortment_group_fk,
                             denominator_id=self.store_id, result=result, score=result,
                             numerator_result=numerator, denominator_result=denominator,
                             identifier_result=kpi_fk)
