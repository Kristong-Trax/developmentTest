
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGCN_PROD.Calculations import PngCNEmptyCalculations
from Projects.PNGCN_PROD.SceneKpis.SceneCalculations import save_scene_item_facts_to_data_provider
from Projects.PNGHK.Calculations import Calculations
from Projects.PNGHK.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd

__author__ = 'ilays'

if __name__ == '__main__':
    LoggerInitializer.init('Png-cn calculations')
    Config.init()
    project_name = 'pngcn-prod'
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    sessions = ['adcda92e-87d9-496f-8472-8a6502711aab',
                '2e71d146-b867-464d-abac-84c3e99efae2']
                # 'ebebc629-6b82-4be8-a872-0caa248ea248',
                # 'cb2cc33d-de43-4c35-a25b-ce538730037e']
    for session in sessions:
        print "Running for {}".format(session)
        # for scene in [16588190]:
        #     print('Calculating scene id: ' + str(scene))
        #     data_provider = KEngineDataProvider(project_name)
        #     data_provider.load_scene_data(session, scene)
        #     output = VanillaOutput()
        #     SceneVanillaCalculations(data_provider, output).run_project_calculations()
        #     save_scene_item_facts_to_data_provider(data_provider, output)
        #     SceneCalculations(data_provider).calculate_kpis()
        data_provider.load_session_data(session)
        output = Output()
        PngCNEmptyCalculations(data_provider, output).run_project_calculations()