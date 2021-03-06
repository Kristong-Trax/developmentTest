import pandas as pd
import numpy as np
from collections import defaultdict
from Projects.RINIELSENUS.Utils.Const import EYELIGHT_KPIS
from Projects.RINIELSENUS.Utils.AtomicKpisCalculator import BlockAtomicKpiCalculation, \
    VerticalBlockAtomicKpiCalculation, AnchorAtomicKpiCalculation, ShelfLevelAtomicKpiCalculation, \
    AdjacencyAtomicKpiCalculation, BlockTargetAtomicKpiCalculation, BiggestSceneBlockAtomicKpiCalculation, \
    LinearFairShareAtomicKpiCalculation, LinearPreferredRangeShareAtomicKpiCalculation, \
    ShareOfAssortmentPrAtomicKpiCalculation, DistributionCalculation, SequenceCalculation, \
    NumOfShelvesCalculation, MiddleShelfCalculation, VerticalSequenceCalculation, ShelfLengthGreaterThenCalculation, \
    ShelfLengthSmallerThenCalculation, ShareOfAssortmentAtomicKpiCalculationNotPR, TwoBlocksAtomicKpiCalculation, \
    NegativeSequenceCalculation, DoubleAnchorAtomicKpiCalculation, SurveyAtomicKpiCalculation, \
    ShelfLevelPercentAtomicKpiCalculation, ShelfLevelSPTAtomicKpiCalculation, LinearFairShareSPTAtomicKpiCalculation, \
    VerticalSequenceAvgShelfCalculation, VerticalBlockOneSceneAtomicKpiCalculation, \
    ShelvedTogetherAtomicKpiCalculation, NegativeAdjacencyCalculation, LinearFairShareNumeratorAtomicKpiCalculation, \
    LinearFairShareDenominatorAtomicKpiCalculation, LinearPreferredRangeShareNumeratorAtomicKpiCalculation, \
    LinearPreferredRangeShareDenominatorAtomicKpiCalculation, ShareOfAssortmentPrNumeratorAtomicKpiCalculation, \
    SequenceSptCalculation, LinearFairShareNumeratorSPTAtomicKpiCalculation, \
    LinearFairShareDenominatorSPTAtomicKpiCalculation, LinearPreferredRangeShareSPTAtomicKpiCalculation, \
    LinearPreferredRangeShareNumeratorSPTAtomicKpiCalculation, \
    LinearPreferredRangeShareDenominatorSPTAtomicKpiCalculation, ShareOfAssortmentPrSPTAtomicKpiCalculation, \
    ShareOfAssortmentPrSPTNumeratorAtomicKpiCalculation, ShelfLengthNumeratorCalculationBase, \
    VerticalPreCalcBlockAtomicKpiCalculation
from Projects.RINIELSENUS.Utils.Const import CalculationDependencyCheck
from Projects.RINIELSENUS.Utils.Fetcher import MarsUsQueries
from Projects.RINIELSENUS.Utils.Const import MPIP_SVR_COLS
from Projects.RINIELSENUS.Utils.Const import FACINGS


class Results(object):
    def __init__(self, tools, data_provider, common, writer, min_face, preferred_range=None):
        self._tools = tools
        self._data_provider = data_provider
        self.common = common
        self._writer = writer
        self._preferred_range = preferred_range
        self.min_face = min_face
        self.dependency_tracker = defaultdict(int)
        self.mpis = self._data_provider['matches']
        self.mpip_sr = self._data_provider._shared_data


    def calculate(self, hierarchy):
        atomic_results = self._get_atomic_results(hierarchy)
        kpi_results = self._get_kpi_results(atomic_results)
        set_result = self._get_set_result(kpi_results)

    def _create_atomic_result(self, atomic_kpi_name, kpi_name, kpi_set_name, result, score=None, threshold=None,
                              weight=None):
        return {'result': result,
                'atomic_kpi_name': atomic_kpi_name,
                'kpi_name': kpi_name,
                'atomic': kpi_set_name,
                'score': score,
                'weight': weight}

    def _get_atomic_results(self, atomics):
        atomic_results = {}
        pushed_back_list = []
        for atomic in atomics:
            # if atomic['atomic'] not in EYELIGHT_KPIS:
            #     continue
            # if atomic['atomic'] not in [
            #                         # 'Is the Nutro Cat Main Meal section >4ft?',
            #                         # 'Is the Nutro Cat Main Meal section <=4ft?',
            #                         # 'Is Nutro Wet Dog food blocked?',
            #                         # 'Is the Meaty Dog Treats segment blocked?',
            #                         # 'Is NUTRO Dry Dog blocked in the Ingredient Transparency feeding philosophy segment?'
            #                         # 'Is PEDIGREE Dry Dog Food shelved with the Basic & Balanced feeding philosophy segment?'
            #                         # 'What percent of non-negotiable items are shelved in the preferred range? - numerator'
            #     # 'Is Nutro Dry Cat food blocked within the Ingredient Transparency feeding philosophy segment?'
            #     # 'Is the Meaty Dog Treats segment blocked?',
            #     # 'Is the Meaty Dog Treats segment blocked vertically?'
            #     # 'What percent of non-negotiable items are shelved in the preferred range?',
            #     'How many feet is the Dog Treats Regular category?',
            #     'Is Pedigree Dentastix Dog Treats blocked within Dental Care?'
            # # 'What percent of non-negotiable items are shelved in the preferred range no pr?'
            #
            # ]:
            #     continue
            # print('~~~~~~~~~~~~~~~~~~~~****************~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            # print(atomic['atomic'])
            if sum([1 for i in atomic['depend_on'] if i is not None and i != '']):
                # if atomic['kpi_type'] == 'PreCalc Vertical Block':
                dependency_status = self._check_atomic_dependency(
                    atomic, pushed_back_list, atomic_results)
                if dependency_status == CalculationDependencyCheck.IGNORE:
                    continue
                elif dependency_status == CalculationDependencyCheck.PUSH_BACK:
                    atomics.append(atomic)
                    pushed_back_list.append(atomic)
                    continue
            if atomic_results:
                r_df = pd.concat(atomic_results.values())
                atomic['results'] = r_df[r_df['atomic'].isin(atomic['depend_on'])]
            calculation = self._kpi_type_calculator_mapping[atomic['kpi_type']](self._tools, self._data_provider,
                                                                                self._preferred_range)
            if self.min_face:
                try:
                    thresh = self.min_face[(atomic['atomic'], self._data_provider.store_type)]['value']
                    if not thresh:
                        thresh = 0
                    atomic['filters'].update({FACINGS: [thresh]})
                except:
                    pass
            # This setup allows some kpis to return an object, so we don't have to
            # keep calculating the same things over and over....
            # if atomic['atomic'] in EYELIGHT_KPIS:
            # self._get_prods_from_filters(calculation, atomic)
            kpi_res = calculation.calculate_atomic_kpi(atomic)
            errata = [None]
            if isinstance(kpi_res, tuple):
                errata = [i for i in kpi_res[1:]]
                kpi_res = kpi_res[0]
            # print('||||| Result for {} is: {}'.format(atomic['atomic'], kpi_res))
            result = {'result': kpi_res,
                      'set': atomic['set'],
                      'kpi': atomic['kpi'],
                      'atomic': atomic['atomic'],
                      'weight': atomic['weight'],
                      'target': atomic.setdefault('target', 0),
                      'errata': errata}
            concat_results = atomic_results.setdefault(
                atomic['kpi'], pd.DataFrame()).append(pd.DataFrame([result]))
            atomic_results[atomic['kpi']] = concat_results

            if not np.isnan(result['result']):
                self._writer.write_to_db_level_3_result(
                    atomic_kpi_name=result['atomic'],
                    kpi_name=result['kpi'],
                    kpi_set_name=result['set'],
                    score=None,
                    threshold=result['target'],
                    result=result['result'],
                    weight=result['weight'])

        return atomic_results

    @property
    def _kpi_type_calculator_mapping(self):
        return {
            SurveyAtomicKpiCalculation.kpi_type: SurveyAtomicKpiCalculation,
            BlockAtomicKpiCalculation.kpi_type: BlockAtomicKpiCalculation,
            TwoBlocksAtomicKpiCalculation.kpi_type: TwoBlocksAtomicKpiCalculation,
            BlockTargetAtomicKpiCalculation.kpi_type: BlockTargetAtomicKpiCalculation,
            BiggestSceneBlockAtomicKpiCalculation.kpi_type: BiggestSceneBlockAtomicKpiCalculation,
            VerticalBlockAtomicKpiCalculation.kpi_type: VerticalBlockAtomicKpiCalculation,
            VerticalBlockOneSceneAtomicKpiCalculation.kpi_type: VerticalBlockOneSceneAtomicKpiCalculation,
            AnchorAtomicKpiCalculation.kpi_type: AnchorAtomicKpiCalculation,
            DoubleAnchorAtomicKpiCalculation.kpi_type: DoubleAnchorAtomicKpiCalculation,
            ShelfLevelAtomicKpiCalculation.kpi_type: ShelfLevelAtomicKpiCalculation,
            ShelfLevelSPTAtomicKpiCalculation.kpi_type: ShelfLevelSPTAtomicKpiCalculation,
            ShelfLevelPercentAtomicKpiCalculation.kpi_type: ShelfLevelPercentAtomicKpiCalculation,
            AdjacencyAtomicKpiCalculation.kpi_type: AdjacencyAtomicKpiCalculation,
            NegativeAdjacencyCalculation.kpi_type: NegativeAdjacencyCalculation,
            LinearFairShareAtomicKpiCalculation.kpi_type: LinearFairShareAtomicKpiCalculation,
            LinearFairShareSPTAtomicKpiCalculation.kpi_type: LinearFairShareSPTAtomicKpiCalculation,
            LinearPreferredRangeShareAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareAtomicKpiCalculation,
            LinearFairShareNumeratorAtomicKpiCalculation.kpi_type: LinearFairShareNumeratorAtomicKpiCalculation,
            LinearPreferredRangeShareNumeratorAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareNumeratorAtomicKpiCalculation,
            LinearFairShareDenominatorAtomicKpiCalculation.kpi_type: LinearFairShareDenominatorAtomicKpiCalculation,
            LinearPreferredRangeShareDenominatorAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareDenominatorAtomicKpiCalculation,
            ShareOfAssortmentPrAtomicKpiCalculation.kpi_type: ShareOfAssortmentPrAtomicKpiCalculation,
            DistributionCalculation.kpi_type: DistributionCalculation,
            SequenceCalculation.kpi_type: SequenceCalculation,
            SequenceSptCalculation.kpi_type: SequenceSptCalculation,
            NegativeSequenceCalculation.kpi_type: NegativeSequenceCalculation,
            ShelfLengthGreaterThenCalculation.kpi_type: ShelfLengthGreaterThenCalculation,
            ShelfLengthSmallerThenCalculation.kpi_type: ShelfLengthSmallerThenCalculation,
            NumOfShelvesCalculation.kpi_type: NumOfShelvesCalculation,
            MiddleShelfCalculation.kpi_type: MiddleShelfCalculation,
            VerticalSequenceCalculation.kpi_type: VerticalSequenceCalculation,
            VerticalSequenceAvgShelfCalculation.kpi_type: VerticalSequenceAvgShelfCalculation,
            ShareOfAssortmentAtomicKpiCalculationNotPR.kpi_type: ShareOfAssortmentAtomicKpiCalculationNotPR,
            ShareOfAssortmentPrNumeratorAtomicKpiCalculation.kpi_type: ShareOfAssortmentPrNumeratorAtomicKpiCalculation,
            ShelvedTogetherAtomicKpiCalculation.kpi_type: ShelvedTogetherAtomicKpiCalculation,
            LinearFairShareNumeratorSPTAtomicKpiCalculation.kpi_type: LinearFairShareNumeratorSPTAtomicKpiCalculation,
            LinearFairShareDenominatorSPTAtomicKpiCalculation.kpi_type: LinearFairShareDenominatorSPTAtomicKpiCalculation,
            LinearPreferredRangeShareSPTAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareSPTAtomicKpiCalculation,
            LinearPreferredRangeShareNumeratorSPTAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareNumeratorSPTAtomicKpiCalculation,
            LinearPreferredRangeShareDenominatorSPTAtomicKpiCalculation.kpi_type: LinearPreferredRangeShareDenominatorSPTAtomicKpiCalculation,
            ShareOfAssortmentPrSPTAtomicKpiCalculation.kpi_type: ShareOfAssortmentPrSPTAtomicKpiCalculation,
            ShareOfAssortmentPrSPTNumeratorAtomicKpiCalculation.kpi_type: ShareOfAssortmentPrSPTNumeratorAtomicKpiCalculation,
            ShelfLengthNumeratorCalculationBase.kpi_type: ShelfLengthNumeratorCalculationBase,
            VerticalPreCalcBlockAtomicKpiCalculation.kpi_type: VerticalPreCalcBlockAtomicKpiCalculation,

        }

    def _get_set_result(self, kpi_results):
        set_results = {}
        for kpi in kpi_results:
            set_results[kpi['set']] = set_results.setdefault(
                kpi['set'], float(0)) + float(kpi['score'])
        for set_, score in set_results.iteritems():
            self._writer.write_to_db_level_1_result(set_, score)

    def _get_kpi_results(self, atomic_results):
        results = []
        for kpi in atomic_results:
            atomic_results[kpi]['weight'].fillna(1, inplace=True)
            atomic_results[kpi]['score'] = atomic_results[kpi]['result']
            kpi_score = atomic_results[kpi].groupby(['kpi', 'set'], as_index=False).agg(
                {'score': 'mean', 'weight': 'sum'})
            score = kpi_score['score'].iloc[0]
            weight = kpi_score['weight'].iloc[0]
            set_ = kpi_score['set'].iloc[0]
            results.append({'score': score, 'set': set_, 'kpi': kpi, 'weight': weight})
            self._writer.write_to_db_level_2_result(kpi, set_, score, weight)
        return results

    @staticmethod
    def _get_group_results(atomic_results):
        group_results = []
        for group in atomic_results:
            group_results.append(
                {'set': group['set'], 'group': group, 'score': atomic_results[group].Score.sum()})

        return pd.DataFrame(group_results, columns=['group', 'score'])

    def _check_atomic_dependency(self, atomic, pushed_back_list, atomic_results):
        depends = zip(atomic['depend_on'], atomic['depend_score'])
        bar = sum([1 for i in atomic['depend_score'] if i is not None and i != ''])
        ret = CalculationDependencyCheck.PUSH_BACK
        score = 0
        if atomic_results:
            results_df = pd.concat(atomic_results.values())
            for depend_on, depend_score in depends:
                if depend_on in results_df['atomic'].tolist():
                    if self.equivalence(results_df[results_df['atomic'] == depend_on]['result'].values[0], depend_score):
                        score += 1
            if score == bar:
                ret = CalculationDependencyCheck.CALCULATE
        if atomic['atomic'] in pushed_back_list or score < bar:
            self.dependency_tracker[atomic['atomic']] += 1
        if self.dependency_tracker[atomic['atomic']] > 5:
            ret = CalculationDependencyCheck.IGNORE
        return ret

    @staticmethod
    def equivalence(actual, expected):
        score = 0
        if actual != '' and actual is not None:
            t = type(actual)
            try:
                expected = t(expected)
            except:
                pass
            if actual == expected:
                score = 1
        else:
            if expected == '' or expected is None:
                score = 1
        return score

    def _get_prods_from_filters(self, calculation, atomic):
        try:
            if 'block' in atomic['atomic']:
                filters = atomic['filters']
            else:
                filters = calculation._split_filters(atomic['filters'])
            allowed = atomic['allowed']
            if atomic['atomic'] == 'Are Meaty Cat Treats blocked?':
                allowed_filter = None
            if atomic['atomic'] == 'Is Nutro Dry Dog food blocked?' or \
                    (atomic['atomic'] == 'Nutro Dry Dog and Wet Dog are BOTH BLOCKED'
                     and filters['Sub-section'] == ['DOG MAIN MEAL DRY']):
                allowed_filter = calculation._get_allowed_products_without_other(allowed, filters)
            else:
                allowed_filter = calculation._get_allowed_products(allowed, filters)
            mpis = self.mpis[self.mpis['stacking_layer'] == 1]
            rel_items = mpis[mpis['product_fk'].isin(calculation._get_filtered_products(filters)[
                                                     'product_fk'])]['probe_match_fk']
            allowed_items = mpis[mpis['product_fk'].isin(
                allowed_filter['product_fk'])]['probe_match_fk']
            # allowed_items = []
            for i, group in enumerate([rel_items, allowed_items]):
                mpip_sr_fk = self.get_mpip_svr_fk(atomic['atomic'], i)
                df = pd.DataFrame(zip(group, [mpip_sr_fk]*len(group)), columns=MPIP_SVR_COLS)
                self.common.match_product_in_probe_state_values = pd.concat(
                    [self.common.match_product_in_probe_state_values, df])
        except Exception as e:
            # print('{} could not generate filters'.format(atomic['atomic']))
            # print(e)
            pass

    def get_mpip_svr_fk(self, kpi, allowed):
        if allowed:
            kpi = '{}_allowed'.format(kpi)
        if len(kpi) > 100:
            kpi = kpi[:100]
        df = self.mpip_sr[self.mpip_sr['name'] == kpi]
        if df.empty:
            if self.mpip_sr.empty:
                self.common.execute_custom_query(MarsUsQueries.add_kpi_to_mvp_sr(kpi, 1))
            else:
                self.common.execute_custom_query(
                    MarsUsQueries.add_kpi_to_mvp_sr(kpi, max(self.mpip_sr['pk'])+1))
            self.mpip_sr = self.common.read_custom_query(MarsUsQueries.get_updated_mvp_sr())
            df = self.mpip_sr[self.mpip_sr['name'] == kpi]
        return df['pk'].values[0]
