
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import Log
from Projects.PERFETTICN_SAND.Calculations import Calculations


if __name__ == '__main__':
    Log.init('perfetticn-sand calculations')
    Config.init()
    project_name = 'inbevbe'
    data_provider = KEngineDataProvider(project_name)
    session = '55bb8f51-33d7-4fca-879d-15cb7785c258'
    data_provider.load_session_data(session)
    output = Output()
    #psdata_provider = PsDataProvider(data_provider,output)
    Calculations(data_provider, output).run_project_calculations()