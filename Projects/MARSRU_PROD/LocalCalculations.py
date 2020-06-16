from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('KEngine')
    project_name = 'marsru-prod'
    session_uids = [

        # 'ffee5551-647f-4552-b296-d5aafb96f842',
        # 'ff9701b2-38b0-4798-8b2b-5578975f8818'
        'aada3bb0-bd76-4b7c-aaa0-0be2a0c4aa7c'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        print session
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()
