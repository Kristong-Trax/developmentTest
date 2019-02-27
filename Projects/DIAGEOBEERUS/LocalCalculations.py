from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOBEERUS.Calculations import Calculations

sessions = [
    'EBB377F6-AAE9-4B70-AC0E-FAD8D891DB29',
    'C682A0F1-0694-40DF-B20C-E5A3E5900E9D',
    'E3426DF8-D8C5-450B-B3CD-654632021BD2',
    'C6ED7E88-2BC3-43C1-ACB9-56CEA107B2D0',
    'CDBABE17-0037-42FC-B70D-B953CC51452B',
    'D2705A22-A0CB-4A47-A0BB-AC1CD2641E0F',
    'D67FEBA2-FF93-4E51-9123-1F6449755DA8',
    'D9CB0EB0-66C1-498D-845D-8E4DE9734C83',
    'E53B015A-F074-45ED-A909-CC8BDD7C52B4',
    'E570D596-3A60-4D89-92A5-F71C481C16DA'
]

sessions = [
    'FCE90A66-4CD9-4D30-9C26-A6524075F1A5'
]

sessions = [
    '12B013B2-5D65-4E1A-A6E6-8A6671BC1796',
    '55FDFE67-4D52-43F2-A7B7-14BC730FB618',
    '96F40321-CDB4-4523-B81B-2CA837121223'
]

sessions = [
    'C682A0F1-0694-40DF-B20C-E5A3E5900E9D'
]

if __name__ == '__main__':
    LoggerInitializer.init('diageobeerus calculations')
    Config.init()
    project_name = 'diageobeerus'
    data_provider = KEngineDataProvider(project_name)
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
