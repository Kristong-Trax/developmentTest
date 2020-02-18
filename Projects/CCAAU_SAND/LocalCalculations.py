

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCAAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccaau-sand calculations')
    Config.init()
    project_name = 'ccaau-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '012C162C-B252-4F09-A5C0-3FB714EDDEFE',
        # '05DB1C22-AD3B-49E1-A004-6EFE70A1E519',
        # '08CDAE94-6765-4F60-B295-E851801E92FC',
        # '730BE6BE-FBE5-41C9-8E34-5C79C70D5E85',
    ]
    for each_sess in sessions:
        print("Running for session ", each_sess)
        data_provider.load_session_data(each_sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()



# DST_Group_NW_SI -- '012C162C-B252-4F09-A5C0-3FB714EDDEFE'
# DST_Group_FS_SI -- '05DB1C22-AD3B-49E1-A004-6EFE70A1E519'
# DST_Group_BF_SI -- '08CDAE94-6765-4F60-B295-E851801E92FC'
# DST_Group_RW_SI -- '730BE6BE-FBE5-41C9-8E34-5C79C70D5E85'