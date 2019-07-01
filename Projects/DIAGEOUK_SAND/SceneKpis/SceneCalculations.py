import os
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from KPIUtils_v2.DB.CommonV2 import Common


class DIAGEOUK_SANDSceneToolBox(SceneBaseClass):
    def __init__(self, data_provider):
        super(DIAGEOUK_SANDSceneToolBox, self).__init__(data_provider)
        # self.scene_generator = SceneGenerator(self._data_provider)
        self.common = Common(self._data_provider)

    def calculate_kpis(self):
        diageo_generator = DIAGEOGenerator(self._data_provider, None, self.common)
        diageo_generator.diageo_global_equipment_score(save_scene_level=True)
        self.common.commit_results_data(result_entity='scene')

