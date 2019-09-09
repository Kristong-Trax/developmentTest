from Trax.Algo.Calculations.Core.KPI.UnifiedKpiSingleton import UnifiedKPISingleton
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Algo.Calculations.Core.DataProvider import Data


class DiageoUtil(UnifiedKPISingleton):

    def __init__(self, data_provider, output=None):
        super(DiageoUtil, self).__init__(data_provider)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.diageo_generator = DIAGEOGenerator(self.data_provider, output, common=None, menu=True)
        self.template_handler = TemplateHandler(self.project_name)
        self.template_handler.update_templates()

    def get_template_data(self, kpi_name):
        template_data = self.template_handler.download_template(kpi_name)
        return template_data
