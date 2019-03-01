from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        # '699149ea-bac9-4677-8ae8-185855014c99',
        # '90d2d5a6-ce79-4af5-91e7-6b75c69c48e6',
        # '839e43e6-1547-4fad-aca4-7324bd93fc87',
        # '2639b610-ce39-4f91-bace-c3a3575e7de8',
        # 'd736c7bf-9be9-4a14-9433-c5d72cb0105a',
        # '109733b0-fc5b-41df-acf5-1e0bd5536bf4',

         'ebe286b9-4200-4f50-8261-21a30c31ea7d',
        # '6626e7a7-05ce-4463-89ee-d94533a5c2d4',
        #
        # '1D6B3586-6A76-4963-BF9F-F8B2E2537ADB',
        # '1ed1f996-a075-4d29-9f1d-5d611ba587cf',
        # '21412B97-9806-46EA-A40C-0A73CE6D4701',
        # '26CAA8DB-00F6-419C-A316-D1ADF8375F3D',
        # '2EBD5DF6-C6E1-4E0A-9395-0F17F39D8728',
        # '4c48617e-6106-427c-bc7c-741feef0ea55',
        # '4CBF89D5-F919-4DB6-9AE9-81294FAA662A',
        # '5234d0c5-2d1b-4aec-a238-ccc9e795ac42',
        # '75408713-cc1a-4828-ab02-189b6aaafd6a',
        # '8F92F832-CEEF-4C33-BE08-6FC2DD16A72E',
        # '90560B95-9D34-494B-B774-69686005EB93',
        # '9AD958CE-F92E-4110-B345-7C965F9FCE3D',
        # '9F2D309B-3672-4774-8A71-E1C5431AB050',
        # 'bf7cf555-db9c-44ea-82cd-905bc7777b31',
        # 'e2a1ce92-907c-479f-aabf-3f5285d0aa35',
        # 'e42f5e34-0b36-44ab-b2b5-9c8442d6ac52',
        # 'E511591F-60A0-4F17-8EBA-D7491C442FF1',
        # 'EE9E423B-40CB-482E-B574-5A53A12DEB8E',
        # 'EFF7B756-457A-4569-96E1-6CD7E7D28EF1',
        # 'FBAEEEC9-9FD7-47B0-8EA0-8F1FCD2EA7D7',

        # 'ffa6f30d-739c-4b74-b1e0-454a89f98ff0',
        # 'FFAD0487-6C6B-4565-A92D-A227CF6EEBAD',
        # 'b0b98c07-583a-40b2-94e2-a8f595f28670',
        #
        # 'FFF338AF-9505-4354-A2B7-835BF78090F3',
        # 'FF81168A-C990-40C2-943D-F68FC9AAACA2',
        # 'ffff2c80-697c-4e61-996e-893142806c55',
        # 'FFF99139-8B36-4272-92CE-E3A9696B819C',
        # 'ffd035af-1396-4b79-a7ac-25210dc8f995',
        # 'fe7080f5-3969-4005-a890-3afe5ffe6960',
        # 'FC8D623A-0DA1-4305-9DBF-9D2CC1301F38',
        # 'FE6D7CDF-161E-46BE-91A3-28B300ED35B0',
        # 'F66CE76B-4B52-4E4D-870D-0A1B0EFCEB17',
        # 'FA6E6AC6-C606-4195-B28C-932B07A289E5',
        # 'FEBEF96B-C8F9-47E4-BDD3-5087D4AB7121',
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
