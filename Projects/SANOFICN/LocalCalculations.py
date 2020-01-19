from Projects.SANOFICN.Calculations import SANOFICNCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

if __name__ == '__main__':
    LoggerInitializer.init('sanoficn calculations')
    Config.init()
    project_name = 'sanoficn'
    data_provider = KEngineDataProvider(project_name)
    session = "A5B576AC-3DEC-4F7A-8217-30D3D4473BC5"
    data_provider.load_session_data(session)
    output = Output()
    SANOFICNCalculations(data_provider, output).run_project_calculations()
