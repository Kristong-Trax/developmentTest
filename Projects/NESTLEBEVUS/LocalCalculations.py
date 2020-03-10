
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEBEVUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestlebevus calculations')
    Config.init()
    project_name = 'nestlebevus'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        # '9ab423e4-c3f4-4930-88fc-6de8ba1a84ff', # had an issue with this

        '9d7f2b8e-c3e1-4ac1-bcb9-a881db1e9ac9',
        'be5fe0fb-dcf1-4be9-aac9-2f29122334e0',
        # '66595e59-5bf6-46b0-8e3c-c1ba48057f5b',
        # '5d86ec63-081f-4977-a1fa-eabe8a626e69',
        # '7d11ca40-3b6a-4ad5-b06a-63ddb4e8aedc',
        # '18620036-6d8d-40df-8f52-2b69ea3bd6e1',
        # '3a179304-09e6-4ea5-9ac6-2f13cbaa341f',
        # '8c9c2a93-748d-48a2-972d-762261202fcb',
        # '311b6a4f-2180-44cc-8d6a-a5d2f703a072',
        # 'b4409ed0-1f97-4db1-871e-d67d891e9021',
        # '343570d8-dc61-45e2-8fc3-228b82b24a7b',
        # '0108eaaa-d590-4877-9bde-db50439b2789',
        # '9695ba07-cf48-4c63-b586-fe6d0717d596',
        # '27132093-522b-4496-a4a8-642dd61e538f',
        # 'b8c666a0-88b6-484c-905e-16adcfc8b1e0',
        # 'ca887a78-84fa-4065-8353-d7275a88efe0',
        # 'e4a8daca-1fa4-4ca4-9f02-059d11e679bd',
        # '490cc8e5-e7ff-404c-b655-8a7d9745d02c',
        # '101ef1c8-d6d2-4e80-a80c-608d16e67b99',
        # '4dbe462c-796c-4236-baa4-9d0b3f04e9f9',
        # '0235d5cf-8ca3-43a9-86b2-6d1398910606',
        # '96e02abf-0fdf-451d-a7b8-40f0fdb62377',
        # '0f18c7aa-ab2f-4b35-a712-ed91ee8116c7',
        # '595b0db0-db06-487c-a2f2-92b963228b01',
        # 'd4c282fe-1c8e-4383-bf72-abbedf2dce7d',
        #
        # '353958e4-840c-487b-9fe2-c67a7538adea',
        #
        # '5ca8ccad-c99a-4929-aaad-9a62f58d6044',
        # '52fbc269-d76b-4dff-878a-f808aa9c3020',
        # '6c01c591-70fe-4067-b816-79d234081773',
        # '16a3346e-9d62-4405-9745-a3feccd960d2',
        # '99993d32-180e-4b68-b1c0-87afff815816',
        # 'a7ede897-31d7-43f5-8f80-1a3a8c0454a0',
        # '528b9bd8-beae-4dc9-862b-2f37fc147887',
        # '1565a9e1-3ae9-41c4-a381-1bf4047e6c4e',
        # 'f268bbe2-8662-4c74-b795-45b28429ff45',
        # 'b1715192-77b7-4ef3-b2d4-1c62c59b0b66',
        # '919b0332-c6f5-4e06-ab95-3d83db5e23a2',
        # '60282c66-bf52-4165-94b1-5c9c0ba63729',
        # 'becea205-3e8f-4a2c-a391-2eadfab61686',
        # '40644c64-731a-4de6-afa7-25523dafd82a',
        # '60262f33-aa3c-4158-9536-c3a3749dfb83',
        # '387ead58-e164-4f34-b1ca-65fb1eb0f2d9',
        # '86136bb6-6ed3-4c42-a39f-b12efa7e7e48',
        # '4e1621d4-7db0-461d-a90e-56b67ecb71d6',
        # '2deda1bb-79f8-41f9-aa91-b59c034308f2',
        # '8f54e399-9377-425a-ae53-1264c3e6f387',
        # '45c8fe5e-090b-4d24-9b9e-63d5024a2c89',
        # '2b4a5e49-9f5d-4431-a379-08a18b5b7d62',
        # 'a64b4369-989f-4f28-99a0-88a345fda435',
        # '9c82e3de-6166-41e8-a019-db457e563384',
        # '6d0e0ddd-8186-48e6-9d26-86bd58ee9b15'
    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
