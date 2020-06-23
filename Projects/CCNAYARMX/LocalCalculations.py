
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx calculations')
    Config.init()
    project_name = 'ccnayarmx'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
        '4d736be9-2c61-4336-b980-4921f2b8f134',
        # '5f20ba65-63ae-4057-a8b0-3491accf7869',
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '0eda0210-b4ed-461c-8b32-09bfddf0cab8',
        '036de825-5353-416d-8347-6b1c5cb1521f',
        # '1c7303e6-96bc-4360-822f-e00886701a1b',
        # '9ba32139-c2bd-4d36-96b3-6268628960ee'
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '69b028e2-5119-4c11-a846-94ca29adede4',
        # 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
        # 'E45618DB-39E9-45A4-B541-054C85515A69'
        # '725524e8-2e9b-4c42-ace1-b2120d987f9e',

        # '725524e8-2e9b-4c42-ace1-b2120d987f9e',
        # '90004fbd-58dd-418f-a359-f2605134291c',
        # '677d0628-9566-4760-8702-821882f74665',
        # 'f6f086d8-b269-4f2d-8229-144af3b1edf8',
        # '7e117743-0448-447b-9ad3-7c895ca8a0b7',
        # '543f7eff-309b-419c-8f39-931cc5cbcba6',
        # '199eda17-4fbb-4c23-b02d-8f3f47e079d9',

        # '086ba425-2332-458e-8eb5-035310d093f5',
        # '0a2f1476-ba74-4c43-a2dc-182d967c08ec',
        # '1a7e6309-a8a1-4912-91bd-898858d33fb6',
        # '1b7d8dc0-7c54-489f-aa85-7e208a52ddaf',
        # '218c0dd7-94df-4ec2-889d-91eebd5c8d6a',
        # '2dc7991d-c322-4797-88ff-b2fad1eeb5fc',
        # '34617a42-1eed-408a-a7b1-baa3af7679dd',
        # '35924bc7-f566-4556-b127-bbf6dc2530b6',
        # '3862280f-3b09-432f-bd11-9563daeea715',
        # '39bfd777-f8bd-4aea-9881-188fecf9772a',
        # '3c105ce2-ca4a-4007-adaa-1f52f65f1f8c',
        # '3ec1785d-7f30-4d55-8629-26ba65932888',
        # '45607081-ecf7-4bb8-b832-cb7097c4df0f',
        # '46456c6e-73ba-4f2e-8caa-03244bb69f0b',
        # '4e6fe667-cec5-4099-be8d-70523f763294',
        # '4f54cd32-1581-4ee0-89d7-f001ed9ba2cc',
        '53011784-1cdb-4545-b06b-2bbee1b677a1',
        '59a57f53-5112-49b3-8b3a-8b9915a9051c',
        '59fd7259-8e1f-40a5-80bd-7065e044128c',
        '5e8d64b6-3450-47a5-84ca-6277ea3e47de',
        '60e87815-edca-4f7c-9840-aaf2a251dcbc',
        '62ca2727-0585-4ebd-b961-a3453124fd2c',
        '669965aa-0cbd-47fc-a916-72ca8db4f64b',
        '682c228e-02b6-4e85-bac5-ab0decde3e30',
        '6dbe8365-8ff8-4c61-96f1-42412aa9a011',
        '7103a481-a6f7-4dd8-a586-92f542897281',
        '71067247-7459-4d9e-9a71-3352f316c80d',
        '7130fa24-b377-45f1-b4f1-d8b86dce1803',
        '713c4aab-bdf6-4244-b5ab-eeeda7bc5ab3',
        '71da1b0a-9e60-4600-90d0-fc8743ee7678',
        '7269ceae-5801-4c68-832c-1d3f268dfb25',
        '73a0a5fa-b658-40e6-b718-8a50007f09ff',
        '75821327-da45-4b75-8687-5cdda99199ca',
        '788753e5-8117-4e8b-8b3f-feee6b8fd83c',
        '7b5c3681-5fb3-4279-b983-84e5c99999e1',
        '818106c3-b457-4f9b-abba-4bc78ba64de7',
        '8679b086-4988-408c-b10f-8a6c5ba83bec',
        '8b80e8a5-4e8c-4afd-85f0-bcfc4f0908fd',
        '90832b25-b899-45e6-b9d1-79b8f6791c1b',
        '9309aa8b-0369-4cc1-a7d6-9989a8c9ec02',
        '93fb2761-f5c8-4054-ad6b-e6b45ac8e4bf',
        '960c13ab-af5d-4722-913e-f6fecf867d9a',
        '9e43b46f-e2c1-4c72-91e6-d36e22e8c258',
        'a2726f04-c2d6-4728-9f85-11b799f1a8b6',
        'a3800442-f2b0-447d-b504-b47c3abbaafc',
        'a82a407d-ffcd-4417-9ecf-c6e506e5e211',
        'aa77faad-9ec4-4c1e-aa95-5891bb9d61cb',
        'ab20d24a-ff3f-4462-a2b6-3fb5f4c45f50',
        'aea27fc4-dc3b-4718-93ab-3181d22843d0',
        'b406b3d7-2b51-4e8e-b42c-494cbd182445',
        'b481ddab-ff54-40a1-8270-e9791034ea75',
        'b659f4d0-fb88-4640-9351-d496f2609e92',
        'bfe5c77f-2251-4209-8358-950000a22322',
        'c01c12e5-aefe-4073-b4cd-71679f10a9f0',
        'c073062f-6a89-4b23-9f50-ff8eb9e70fcb',
        'c59969d0-167c-4ca1-8a75-90d3bb5e91dc',
        'c64344ec-d15d-42ce-ab0c-267ce53a1346',
        'c765a54e-66e7-4b80-98b7-9f7ae809c20d',
        'c7b7f4f3-28a7-4c1f-986e-9cf6c0e9175b',
        'c9d44b23-f35c-47b8-a435-eb6f4393db93',
        'ce6b504f-8d20-4324-8f45-1fa88c2bdbc9',
        'd05bde1f-2d37-4c24-b626-da29d384ac30',
        'd46e2f0e-da92-4dd5-9ce9-f18df2758b2c',
        'd4eb1c88-3452-4e3e-ab34-231c4b240772',
        'd50b493e-7632-4dc4-b740-501da23ff58e',
        'db17ea97-59ef-40be-8c31-0f1ca106277a',
        'db7be435-12da-4213-87e0-65f2c9291b1a',
        'dba12279-eb51-42b7-927b-13557eb9b52f',
        'df1b97cd-3c16-44fe-9c42-95ee103f233c',
        'e80eae79-b9cb-4875-a988-6fb67769b41c',
        'f1bd1217-74d4-4e0e-a0bf-be247eab988b',
        'f1c93102-4594-49d7-aae8-151dd65851b8',
        'fa89a4db-688a-4228-adee-d5a36d508936',
        'fac4313c-acd5-42bc-9eb8-24e05d3ccbe4',
    ]

    for session in session_list:
        print("==================== {} ====================".format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
