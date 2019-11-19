
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
                'd16a0359-4d0f-4fed-bfe4-0afecca6843e': ['14431058']
                # 'd16a0359-4d0f-4fed-bfe4-0afecca6843e': ['14431283', '14431246', '14431264'],
                # 'c3b7fd33-8ff5-40fb-bf9a-5e7b58193dd0': ['14433134'],
                # '077c0582-76d8-4f96-861a-6c2b52787900': ['14436056'],
                # '1a9bfc80-db62-4ca1-a412-ce68de389805': ['14431888'],
                # '1ab8d620-93ac-4efc-b115-b99bed743fd1': ['14439705'],
                # 'bbe0262f-8565-4bd3-8b6a-6352ada59daa': ['14435972'],
                # '050a5b18-acd1-4c4d-867a-e833368d8cdc':[],
                # '7d9bca6b-c3a6-4c74-901c-8dffd151d551':[]

               }
                # '381DD222-229E-4B87-ADCD-545B9531D7F2': ['19626212'],
                #         '4B69AD0D-64BE-4A14-BBE1-7FF025E7C12F': ['19626218'],
                # 'b958b316-8508-4455-bb25-7903bdbdaf5b': ['19626396', '19626413'],
                # '9CA65502-74CF-400D-B24B-E70496D3EE02': ['17910469'],
                # 'b4891c75-ccd5-4da6-bb4c-e33e433eb18d': ['19625867']}
    # '10EAADFA-FF0D-488A-8650-2E8A4E2CBAC0': ['17915480']}
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
