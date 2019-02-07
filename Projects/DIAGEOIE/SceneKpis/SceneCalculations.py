import os
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
# from Projects.DIAGEOIE.KPISceneGenerator import SceneGenerator
from KPIUtils_v2.DB.CommonV2 import Common


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        # self.scene_generator = SceneGenerator(self._data_provider)
        self.common = Common(self._data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        # self.scene_generator.scene_score()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                     'Data', 'Brand Score.xlsx')
        diageo_generator = DIAGEOGenerator(self._data_provider, None, self.common)
        diageo_generator.diageo_global_tap_brand_score_function(template_path=template_path, rank_kpi=False,
                                                                sub_category_rank_kpi=False, save_scene_level=True,
                                                                calculate_components=True)
        self.common.commit_results_data(result_entity='scene')
        # self.timer.stop('KPIGenerator.run_project_calculations')
