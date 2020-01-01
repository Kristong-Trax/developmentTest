from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DB import SceneResultsConsts, SessionResultsConsts


class SosBrandOutOfManufacturer(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SosBrandOutOfManufacturer, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return'SOS BRAND OUT OF MANUFACTURER'

    def calculate(self):
        """
        aggregate over scene results.
        """
        # add the dependincy 9000
        # level 5
        OOTB_Data = self.dependencies_data
        res_list = []
        sos_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('SOS BRAND OUT OF MANUFACTURER')
        Agg_Results = OOTB_Data.groupby([SessionResultsConsts.NUMERATOR_ID,SessionResultsConsts.DENOMINATOR_ID],
                                        as_index=False).agg(  # aggregate by numerator id
            {'fk': lambda x: tuple(x)[0], SessionResultsConsts.NUMERATOR_RESULT:"sum",
             SessionResultsConsts.DENOMINATOR_RESULT: lambda x: tuple(x)[0], SessionResultsConsts.RESULT: 'sum',
             SessionResultsConsts.WEIGHT: lambda x: tuple(x)[0], SessionResultsConsts.SCORE: 'sum',
             SessionResultsConsts.SCORE_AFTER_ACTIONS: lambda x: tuple(x)[0],
             'kpi_definition_fk': lambda x: tuple(x)[0], SessionResultsConsts.CONTEXT_ID: lambda x: tuple(x)[0],
             SessionResultsConsts.TARGET: lambda x: tuple(x)[0]})
        # Agg_Results.drop_duplicates(inplace=True)
        for index, row in Agg_Results.iterrows():
            res_list.append(self.util.build_dictionary_for_db_insert_v2(fk=sos_fk, numerator_id=row[
                SceneResultsConsts.NUMERATOR_ID],
                                                                        numerator_result=row[
                                                                            SceneResultsConsts.NUMERATOR_RESULT],
                                                                        denominator_id=row[
                                                                            SceneResultsConsts.DENOMINATOR_ID],
                                                                        denominator_result=row[
                                                                            SceneResultsConsts.DENOMINATOR_RESULT],
                                                                        target=row[SceneResultsConsts.TARGET],
                                                                        result=row[SceneResultsConsts.RESULT],
                                                                        score=row[SceneResultsConsts.SCORE],
                                                                        context_id= row[SceneResultsConsts.CONTEXT_ID]))
        for res in res_list:
            self.write_to_db_result(**res)