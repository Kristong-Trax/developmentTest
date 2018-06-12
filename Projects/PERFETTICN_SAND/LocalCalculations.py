
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import Log
from Projects.PERFETTICN_SAND.Calculations import Calculations


if __name__ == '__main__':
    Log.init('perfetticn-sand calculations')
    Config.init()
    #project_name = 'diageotw'
    project_name = 'perfetticn-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'B57B5139-3D03-4ABB-8043-0DCE260A11E1'
    session2 =   '250CD4BC-5CDD-4DA5-9D6B-67022B4BDE8B'
    #session='3CEF75C7-1818-4969-A227-F51FF72B3C9F'
    data_provider.load_session_data(session2)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()