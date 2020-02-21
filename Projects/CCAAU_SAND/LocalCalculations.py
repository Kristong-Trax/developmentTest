

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCAAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccaau-sand calculations')
    Config.init()
    project_name = 'ccaau-sand'
    data_provider = KEngineDataProvider(project_name)
    # DST_Group_NW_SI -- '012C162C-B252-4F09-A5C0-3FB714EDDEFE'
    # DST_Group_FS_SI -- '05DB1C22-AD3B-49E1-A004-6EFE70A1E519'
    # DST_Group_BF_SI -- '08CDAE94-6765-4F60-B295-E851801E92FC'
    # DST_Group_RW_SI -- '730BE6BE-FBE5-41C9-8E34-5C79C70D5E85'
    sessions = [
        "012C162C-B252-4F09-A5C0-3FB714EDDEFE",  # used in TEST
        # "FFFB74D4-3988-4558-B3F7-09FFA57D4378",  # curr ##
        # "98A69AA5-36E1-4776-B4D3-6A24D189FCAD",  # prev ##
        # "FFFD9325-4A97-47AE-BACC-94A0D58628EC",  # sayanis session
        # "8ab44317-3dbc-4170-bc0b-f389bd407902",  # prev of 3FB714EDDEFE
        # "08CDAE94-6765-4F60-B295-E851801E92FC",
        # "730BE6BE-FBE5-41C9-8E34-5C79C70D5E85",
        # # # # # excluded '05DB1C22-AD3B-49E1-A004-6EFE70A1E519', '2633487', 'CCA-Standard checkout cooler'
        # "05DB1C22-AD3B-49E1-A004-6EFE70A1E519",
    ]
    # "A8BC5873-311B-4D92-813D-F58888374BD5" this needs tmeplate v2
    for each_sess in sessions:
        print("Running for session ", each_sess)
        data_provider.load_session_data(each_sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
