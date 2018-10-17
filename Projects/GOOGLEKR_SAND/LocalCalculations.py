
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEKR_SAND.Calculations import GOOGLEKR_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('googlekr-sand calculations')
    Config.init()
    project_name = 'googlekr-sand'
    sessions = [
        # '0002f34c-c186-11e8-b150-12499d9ea556',  # empty session for error testing
        '00581f3d-c25d-11e8-b150-12499d9ea556',
        '0ad9a74c-c2dc-11e8-b150-12499d9ea556',
        '0d3f9fc8-c155-11e8-b150-12499d9ea556',
        # '15578a99-c186-11e8-b150-12499d9ea556',
        # '16bb102b-c0bb-11e8-b150-12499d9ea556',
        # '1718cb09-c25d-11e8-b150-12499d9ea556',
        # '1b7ecd17-c095-11e8-b150-12499d9ea556',
        # '2c3cceaf-c25d-11e8-b150-12499d9ea556',
        # '2c4fda9b-c2dc-11e8-b150-12499d9ea556',
        # '365a20f8-c2dc-11e8-b150-12499d9ea556',
        # '3853cb53-c20e-11e8-b150-12499d9ea556',
        # '3d0b9c1a-bb2b-11e8-b150-12499d9ea556',
        # '4f6b4ced-c2dc-11e8-b150-12499d9ea556',
        # '5951864c-c25d-11e8-b150-12499d9ea556',
        # '5adfc053-c2dc-11e8-b150-12499d9ea556',
        # '5df38355-c185-11e8-b150-12499d9ea556',
        # '67c1a58e-c2dc-11e8-b150-12499d9ea556',
        # '6c0cf9c5-c20e-11e8-b150-12499d9ea556',
        # '71e6adc6-c153-11e8-b150-12499d9ea556',
        # '72f45bce-c185-11e8-b150-12499d9ea556',
        # '779b6879-c0a3-11e8-b150-12499d9ea556',
        # '77cc255d-c22b-11e8-b150-12499d9ea556',
        # '81bbfa91-c25d-11e8-b150-12499d9ea556',
        # '857ef877-c2dc-11e8-b150-12499d9ea556',
        # '8b394c78-c185-11e8-b150-12499d9ea556',
        # '8e79c979-c25c-11e8-b150-12499d9ea556',
        # '949d62f4-c145-11e8-b150-12499d9ea556',
        # '982eaace-c2dc-11e8-b150-12499d9ea556',
        # 'a17f09cb-c185-11e8-b150-12499d9ea556',
        # 'a30e85d1-c2dc-11e8-b150-12499d9ea556',
        # 'a4b2313c-c25c-11e8-b150-12499d9ea556',
        # 'b230ce14-c2dc-11e8-b150-12499d9ea556',
        # 'b730d6fe-c185-11e8-b150-12499d9ea556',
        # 'bbd2d9aa-c25c-11e8-b150-12499d9ea556',
        # 'c143a21a-c142-11e8-b150-12499d9ea556',
        # 'c732de44-c08e-11e8-b150-12499d9ea556',
        # 'ca867d7c-c185-11e8-b150-12499d9ea556',
        # 'cfe40e0d-c153-11e8-b150-12499d9ea556',
        # 'd20a2bdb-c25c-11e8-b150-12499d9ea556',
        # 'd3be10bf-c093-11e8-b150-12499d9ea556',
        # 'db3ed661-c146-11e8-b150-12499d9ea556',
        # 'e78388ea-c25c-11e8-b150-12499d9ea556',
        # 'ec0108db-c185-11e8-b150-12499d9ea556',
    ]

    sessions = ['3985c4f3-8e86-4cd0-8455-9202306e7d3e',
                'A7330F2D-620A-4821-A62C-329CFB45D867',
                'A4CDE6E6-9145-484C-9894-3B78B08085CA',
                '78949CC4-5E67-47CF-A043-ED832497C1C5',
                '924d3ce8-10f1-4083-9aa6-39b023eca812',
                '9595AA6E-CB9C-49DE-B043-0920DCF9EEE0',
                'f3a1a34e-043d-4c8b-99a5-3f9b7f9e0847',
                '7c73be34-20b9-4d6c-b48f-ec4a04745660',
                '5E35A64E-6B49-4106-AC14-BFBFC6BFF4DB',
                'ECFEEC98-4C2C-46F6-851F-97DBE14A3254',
                'C23CF630-5953-48A3-AE82-FF1E5BE0C8E0',
                '698CC197-6902-42AF-8015-AF6FD0DAF8D1',
                ]

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        GOOGLEKR_SANDCalculations(data_provider, output).run_project_calculations()
