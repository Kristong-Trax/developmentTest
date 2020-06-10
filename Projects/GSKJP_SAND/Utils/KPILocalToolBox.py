from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts

__author__ = 'prasanna'


class GSKLocalToolBox(GSKToolBox, object):

    def __init__(self, data_provider, output, common, set_up_file):
        super(GSKLocalToolBox, self).__init__(data_provider, output, common, set_up_file)

    def assortment_lvl3_gsk_adjustments1(self):
        super(GSKLocalToolBox, self).assortment_lvl3_gsk_adjustments1()

        if self.assort_lvl3 is None or self.assort_lvl3.empty:
            return

        # TODO: Multiple-granular groups for a store's policy - Remove Duplicate product_fks if Any
        Log.info("Dropping duplicate product_fks across multiple-granular groups")
        Log.info("Before : {}".format(len(self.assort_lvl3)))
        self.assort_lvl3 = self.assort_lvl3.drop_duplicates(subset=[ProductsConsts.PRODUCT_FK])
        Log.info("After : {}".format(len(self.assort_lvl3)))
