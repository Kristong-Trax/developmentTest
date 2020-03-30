
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGCN_SAND.Calculations import PngCNEmptyCalculations
from Projects.PNGCN_SAND.SceneKpis.SceneCalculations import save_scene_item_facts_to_data_provider
from Projects.PNGCN_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd

__author__ = 'ilays'

if __name__ == '__main__':
    LoggerInitializer.init('Png-cn calculations')
    Config.init()
    project_name = 'pngcn-sand'
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    sessions = {

        'EE913E9B-71E7-4A39-A66F-2A9B8DA9B2BB': ['27846171', '27846517', '27848213', '27848280'],
        '9be106d4-24a3-4876-bfe1-b096510770fd': ['27860731', '27860735', '27860737', '27860729', '27860772', '27860775',
                                                 '27860790', '27860727', '27860784', '27860789'],
        # 'ee91e5a6-f2c6-41f5-b9ad-e1ba865980cc': ['27886925', '27886943'],
        # '31829C7D-C7D7-453A-BF54-1163EDC0C4DE': ['27800831', '27800963', '27800980', '27800995', '27801020',
        #                                          '27801122'],
        # '75D1AB6C-5A7F-4DBD-B220-0938CAB0B2FC': ['27802705', '27802765', '27802775', '27802791', '27802804', '27802814',
        #                                          '27802819', '27802825', '27802845', '27802909'],
        'e8c67437-814e-48cb-bc66-fb8bbf7bf7f9': ['25728621'],
    }
    for session in sessions.keys():
        print "Running for {}".format(str(session))
        for scene in sessions[session]:
            print('Calculating scene id: ' + str(scene))
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene)
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()
        # data_provider.load_session_data(session)
        # output = Output()
        # PngCNEmptyCalculations(data_provider, output).run_project_calculations()
