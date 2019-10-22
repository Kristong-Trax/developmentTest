
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
        "22D01810-B4CB-40F7-B81A-6FD2E7E37F0A",
        "39684667-5A98-4EDF-9441-5D8E6E6F3FAD",
        "33C2755D-C40A-4779-AB71-46E6F7747E38",
        "D7DD0327-1165-43D7-AF7C-BF4828ACAF95",
        "EF36B2AD-CD25-4BB8-870B-A9E7CC3DFC41",
        "1BF7E595-76A1-4232-9875-9233B69F5093"
    ]
    for session in sessions:
        print "Running session >>", session
        data_provider.load_session_data(session)
        output = Output()
        SANOFIJPCalculations(data_provider, output).run_project_calculations()
        print "*******************************"
