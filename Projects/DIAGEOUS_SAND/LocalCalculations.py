
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous-sand'
    sessions = [
        # "004F1CF3-7135-44FD-9651-5F1E9E4C0BB6",
        # "039CA55C-2629-4055-BF3B-6ABBB7FCCC27",
        # "6CA23F24-9A52-4D63-9594-BF202C81AEF0",
        # "18947C84-7F68-40D5-B0D1-0674DABE3597",
        # "013C844D-63A4-4A67-A8C2-7CB26F875E64",
        # "094D7267-1826-4F87-8698-D6697FCA3C66",
        # "F8B8C483-1C4A-45A5-AB61-7AF898713837",
        # "2307C486-FB2F-4163-B0F9-F32ECE64A169",
        # "0F52CDEA-C2B1-4D24-A9B2-0785AE35CE7F",
        # "FB7CB608-B781-4528-A95B-2EE981F6B53B",
        # "01D1A205-C92D-43E4-8139-D6B66AE945BC",
        # "BF6F26A6-F8CB-41B3-905B-A8C7604FF824",
        # "2B7BE013-0732-499A-B710-2C65D3EE81CA",
        # "E578C1B1-0B24-4957-B20E-5FD07EA3604A",
        # "2EB2B038-3B04-431B-8AF9-48639A2D802D",
        # "B12B9174-E40B-405F-92D9-DAAC20EB8B0A",
        # "7C6EE177-402B-461C-A173-28F19C6D7D34",
        "A5CE869F-8344-4D48-973A-A60CA78E943C",
        # "3ACBE5E5-D4FD-41E6-BA9B-B7A69DC799E3",
        # "EDFFE77D-557E-4048-8D22-C4772C00C595",
        # "C84AB9A7-7057-43EE-B4EC-68EF945D10BD",
        # "0127D4DC-C052-46A7-9482-EA4F2F02684A",
        # "F2F1026C-6603-4A08-8537-1FEE2BFF4971",
        # "C5BAC7FE-BE9D-4E9A-A7A6-6D3E5A1D349C",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
