
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGCN_PROD.Calculations import PngCNEmptyCalculations
from Projects.PNGCN_PROD.SceneKpis.SceneCalculations import save_scene_item_facts_to_data_provider
from Projects.PNGCN_PROD.SceneKpis.SceneCalculations import SceneCalculations
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
    sessions = {
                '471D9B25-30FA-4B33-97DC-509D126E527E': ['18478945'],
                '117BC751-52D2-462E-8EF4-4ECE6187179C': ['19214788']}
                # {'0aafdc01-51bb-46cb-bf22-33ccc4a27325': ['18542929'],
                # '117BC751-52D2-462E-8EF4-4ECE6187179C': ['19214768']}
                # '6AC1E27A-B2C7-4389-8522-7F1D29642CA4': ['17901923'],
                # '0D9C3FAE-E62D-4467-B279-2FB8FC32A2DD': ['17886164'],
                # '6e4dc935-ab56-45ef-9408-caaddb963874': ['17888505'],
                # 'C544B5DB-B61F-4B02-B03A-6D8748B3B636': ['17874115']}
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
        data_provider.load_session_data(session)
        output = Output()
        PngCNEmptyCalculations(data_provider, output).run_project_calculations()