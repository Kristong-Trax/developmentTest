
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JTIMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jtimx calculations')
    Config.init()
    project_name = 'jtimx'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        '9a1749f4-1480-4210-ab52-dfacacc4bdc0',
        #'8c80f46d-b7de-41cb-af8a-b311056354ec',
        # '6b85a079-a417-4601-9780-f9f96cd960a3'
        '00f9febe-26fa-4abc-b822-790bbdb341bc',
        '02f87384-7af5-4f01-88f3-afde526831ac',
        '12829f49-3c4e-4b56-abba-911a751a9310'
        # '5eecf0b7-5e05-48a3-9bf7-50bb545196ae',
        #             'fb3d78b5-c256-4ef0-a672-1e03fb1b09fa',
        #             'fcfeb334-04f1-4ad6-86b9-7a7632a67b6f',
        #             'f6e61f8a-f548-42a0-b1f6-57f55ee4be24',
        #             'f8d5ba8f-2c7c-406f-a07c-009e3240dcd4',
        #             'f8c5cc87-9722-4dff-974f-ef35af208e25',
        #             'fa691868-1d26-444a-b10f-8314b4f876ae',
        #             'f736084c-1ec8-4425-82f9-4e492bf22ab5',
        #             'f902aa4c-3988-430c-a59b-dba07542ec41',
        #             'f82e349c-cbea-4777-a476-9804f4fd7fd4',
        #             'f7fc0eb7-4414-4732-a945-340bb84daeaf'
                    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
