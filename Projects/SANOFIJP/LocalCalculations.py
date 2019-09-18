
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SANOFIJP.Calculations import SANOFIJPCalculations

if __name__ == '__main__':
    LoggerInitializer.init('Sanofi Japan Calculations')
    Config.init()
    project_name = 'sanofijp'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 2019-06-13
        "0030CDE1-DBE0-4609-8FF2-958C1797BA2C",
        "02ACE947-180B-4167-A355-A46C7F8C39E2",
        "04FF22BE-CB45-4DFC-A80D-39EA616414D2",
        "0684F16C-A332-4100-BC7A-93405602BC82",
        "0889811D-9B25-453D-B7C3-812DA25C0ABC"
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        SANOFIJPCalculations(data_provider, output).run_project_calculations()
        print "*******************************"
