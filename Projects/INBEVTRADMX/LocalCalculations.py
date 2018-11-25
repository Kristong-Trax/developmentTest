
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVTRADMX.Calculations import INBEVTRADMXCalculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevtradmx calculations')
    Config.init()
    project_name = 'inbevtradmx'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 'ffffb22d-93e6-4b28-9ae3-788c7449fdd6',
        # 'fffcf114-9584-4526-b0d2-efeeccf519b8',
        # 'fff7c1b3-62ea-488e-9541-38458ce53c81',
        # 'ffe38f10-2f22-4458-b805-ab1c4dbb3872',
        # 'ffd80fd1-a3d1-4114-98b9-111f3da7dce1',
        # 'ffd62bba-96c4-48b2-b8f5-8ef309614013',
        # 'ffc43897-cabe-4635-a284-c0867f42bddc',
        # 'ffc3f7ec-b47a-4ad6-a5b0-c0e5d5369c00',
        # 'ffa06992-4650-4cc1-bc37-9bf7c4efbdef',
        # 'ff693125-5a76-49f6-8b94-05748fb01b55',
        # 'ff5c2502-d67a-42fc-b934-207411862e97',
        # 'ff5bc377-04b8-4419-90bc-4a74d894b8f5',
        # 'ff391525-762c-475d-90a7-3eed05f34a40',
        # 'ff354a09-a766-419a-96d8-6aaa8fad2c98',
        # 'fedacc33-e947-426f-ad52-78741461ed51',
        # 'f7fc132d-ccfd-4cb3-8820-ad9b0248ccb9',
        # 'f63b661d-bd5f-4100-8ae4-c3346ee5f268',
        # 'f30b62b5-1ff7-4c64-a73e-d0f87c457b2c',
        # 'f3003b62-cfb6-4da8-8a0e-5e340cabfcdf',
        # 'ebfd93ad-597d-469d-b7a0-4d091e21d060',
        # 'dfb30511-729b-4979-9440-cfa68b3f69bb',
        # 'cbe15644-9da0-4fe3-8362-fb61237f0376',
        'fa7b54b8-f4c3-4864-9f09-3c06612ba7b6'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        INBEVTRADMXCalculations(data_provider, output).run_project_calculations()
