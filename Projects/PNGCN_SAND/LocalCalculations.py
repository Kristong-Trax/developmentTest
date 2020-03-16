
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
        'e8c67437-814e-48cb-bc66-fb8bbf7bf7f9': ['25728621'],
        # '12fbee76-d5e1-4fac-8b0d-9c9fb79d9fa9': ['27809888', '27809910', '27810432', '27810677'],
        # '9be106d4-24a3-4876-bfe1-b096510770fd': ['27832235', '27832236', '27832238'],
        # 'FEF1A8AA-D4EF-4CAB-810E-C67AB06906F1': ['27808870', '27809379', '27809391', '27809928', '27811752'],
        # '4974a7fe-79f5-42e5-a21a-08d633aa8a88': ['27809184', '27809190', '27809197'],
        # 'c274e6ee-803b-430c-9f34-da68c74666e0': ['27822271', '27822583', '27822620'],
        # '6a18c7a0-0d32-47fe-8120-028ca2bd01db': ['27830783', '27830827', '27830859', '27830887', '27830892',
        #                                          '27830900'],
        # '7e4b7eb3-515e-4451-a1bf-0331136cac51': ['27818693', '27818735', '27818896'],
        # 'b8dfe74e-72bf-4e95-81c9-1369ed8b0d26': ['27831039', '27831040', '27831042', '27831043', '27831044'],
        # '139b00c9-9de2-4d38-b6e0-1a4cdab60a37': ['27835858', '27835859', '27835860', '27835861', '27835862', '27835863',
        #                                          '27835866', '27835867', '27835868', '27835869'],
        # 'EF8A76C2-0340-4EC7-BB20-F47606FE2977': ['27841632', '27840962', '27841804', '27841655'],
        # '7255A527-2813-447B-84E2-58EF2DF7C502': ['27831687', '27831685', '27831686', '27831699', '27831700'],
        # '8537189C-D8BD-4E37-A1D6-DFCE2EA6839B': ['27826946', '27826952', '27826960'],
        # '413A80A5-51CD-487A-9BE8-1DABE5ADF376': ['27827364', '27827732', '27827371', '27827340'],
        # '91635680-EEFC-4948-9712-48AF19E573C8': ['27824489', '27824471', '27824470', '27824844']

        # one shelf
        # '5a1862d6-ed90-4fd9-a7bd-8f5186f293e7': ['28111603'],
    }
    for session in sessions.keys():
        print "Running for {}".format(str(session))
        for scene in sessions[session]:
            try:
                print('Calculating scene id: ' + str(scene))
                data_provider = KEngineDataProvider(project_name)
                data_provider.load_scene_data(session, scene)
                output = VanillaOutput()
                SceneVanillaCalculations(data_provider, output).run_project_calculations()
                save_scene_item_facts_to_data_provider(data_provider, output)
                SceneCalculations(data_provider).calculate_kpis()
            except Exception as e:
                print ("Scene {} failed to calculate with error {}".format(scene, e))
        # data_provider.load_session_data(session)
        # output = Output()
        # PngCNEmptyCalculations(data_provider, output).run_project_calculations()
