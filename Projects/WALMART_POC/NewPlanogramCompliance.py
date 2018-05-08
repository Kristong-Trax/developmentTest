from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
from Trax.Apps.Services.CalculationsEngine.Handlers.PlanogramCompliance.NeedlemanWunsch import needleman_wunsch
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.WALMART_POC.KPIToolBox import KPIToolBox

__author__ = 'ortalk'


class WalmartCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        tool_box = KPIToolBox(self.data_provider, self.output)
        scenes = tool_box.scenes_info['scene_fk'].unique().tolist()
        for scene in scenes:
            needleman_wunsch(project_name, scene)
        self.timer.stop('WalmartCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('Walmart calculations')
#     Config.init()
#     project_name = 'walmart-poc'
#     data_provider = ACEDataProvider(project_name)
#     data_provider.load_session_data('3ac5eec3-2f2c-4b2e-94e2-2ccff049f33f')
#     output = Output()
#     WalmartCalculations(data_provider, output).run_project_calculations()