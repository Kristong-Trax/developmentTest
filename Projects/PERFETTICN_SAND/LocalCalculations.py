
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import Log
from Projects.PERFETTICN_SAND.Calculations import Calculations


if __name__ == '__main__':
    Log.init('perfetticn-sand calculations')
    Config.init()
    #project_name = 'diageomx-sand'
    project_name = 'perfetticn-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '86F603CB-11E1-4387-AC63-DD4C14F64F74'
    #session='69ea07bc-34c3-478f-88a9-e7f6859e61c1'
    # session='9b0b74c8-c72c-420e-938c-3cd4375708c5'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()