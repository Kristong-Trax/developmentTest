from KPIUtils.Calculations.Assortment import Assortment
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from OutOfTheBox.Calculations.SOSBase import BaseFieldRetriever
from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Algo.Calculations.Core.DataProvider import Data


class DiageoUtil(UnifiedKPISingleton):

    def __init__(self, data_provider, output=None):
        super(DiageoUtil, self).__init__(data_provider)
        self.data_provider = data_provider
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.diageo_generator = DIAGEOGenerator(self.data_provider, output, common=None, menu=True)
        self.diageo_manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.template_handler = TemplateHandler(self.data_provider.project_name)
        self.template_handler.update_templates()
        self.assortment = Assortment(self.data_provider)
        self.assortment_lvl3_results = self.assortment.calculate_lvl3_assortment()

    @property
    def assortment_lvl2_results(self):
        if self.assortment_lvl3_results:
            return self.assortment.calculate_lvl2_assortment(self.assortment_lvl3_results)
        return None

    def get_template_data(self, kpi_name):
        template_data = self.template_handler.download_template(kpi_name)
        return template_data


class SimpleFacingsRetriever(BaseFieldRetriever):
    @property
    def sos_field(self):
        return ScifConsts.FACINGS


class DiageoConsts(object):
    SECONDARY_DISPLAYS = ['Secondary', 'Secondary Shelf']
