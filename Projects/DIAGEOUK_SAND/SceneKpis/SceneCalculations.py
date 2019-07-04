import os
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from KPIUtils_v2.DB.CommonV2 import Common


class DIAGEOUK_SANDSceneToolBox(SceneBaseClass):
    def __init__(self, data_provider):
        super(DIAGEOUK_SANDSceneToolBox, self).__init__(data_provider)
        # self.scene_generator = SceneGenerator(self._data_provider)
        self.common = Common(self._data_provider)
        self.diageo_generator = DIAGEOGenerator(self._data_provider, None, self.common)

    def calculate_kpis(self):
        # diageo_generator = DIAGEOGenerator(self._data_provider, None, self.common)
        if self.is_scene_relevant():
            print self.diageo_generator.scif['scene_fk'].values
            self.diageo_generator.diageo_global_equipment_score(save_scene_level=True)
        if not self.common.kpi_results.empty:
            self.common.commit_results_data(result_entity='scene')

    def is_scene_relevant(self):
        if self.diageo_generator.scif.empty:
            return False
        else:
            return True if self.diageo_generator.scif['template_name'].values[0] == 'ON - DRAUGHT TAPS' else False

