import pandas as pd
import numpy as np

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
    SequenceSptCalculation
from Projects.RINIELSENUS.Utils.Const import CalculationDependencyCheck


class Results(object):
    def __init__(self, tools, data_provider, writer, preferred_range=None):
        self._tools = tools
        self._data_provider = data_provider
        self._writer = writer
        self._preferred_range = preferred_range

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
            if not ('Does the Dog Treats category lead the Dog Food aisle?' in atomic['atomic'] or\
                    'How many feet is the Dog Treats Regular category?' in atomic['atomic']):
                continue
            print(atomic['atomic'])
            if atomic['depend_on']:
                dependency_status = self._check_atomic_dependency(atomic, pushed_back_list, atomic_results)
                if dependency_status == CalculationDependencyCheck.IGNORE:
                    continue
                elif dependency_status == CalculationDependencyCheck.PUSH_BACK:
                    atomics.append(atomic)
                    pushed_back_list.append(atomic)
                    continue

            calculation = self._kpi_type_calculator_mapping[atomic['kpi_type']](self._tools, self._data_provider,
                                                                                self._preferred_range)
            result = {'result': calculation.calculate_atomic_kpi(atomic),
                      'set': atomic['set'],
                      'kpi': atomic['kpi'],
                      'atomic': atomic['atomic'],
                      'weight': atomic['weight'],
                      'target': atomic.setdefault('target', 0)}
            concat_results = atomic_results.setdefault(atomic['kpi'], pd.DataFrame()).append(pd.DataFrame([result]))
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
            ShelvedTogetherAtomicKpiCalculation.kpi_type: ShelvedTogetherAtomicKpiCalculation
        }

    def _get_set_result(self, kpi_results):
        set_results = {}
        for kpi in kpi_results:
            set_results[kpi['set']] = set_results.setdefault(kpi['set'], float(0)) + float(kpi['score'])
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
            group_results.append({'set': group['set'], 'group': group, 'score': atomic_results[group].Score.sum()})

        return pd.DataFrame(group_results, columns=['group', 'score'])

    @staticmethod
    def _check_atomic_dependency(atomic, pushed_back_list, atomic_results):
        depend_on = atomic['depend_on']
        depend_score = atomic['depend_score']
        results_df = pd.concat(atomic_results.values())
        if depend_on in results_df['atomic'].tolist():
            if results_df[results_df['atomic'] == depend_on]['result'].values[0] == depend_score:
                return CalculationDependencyCheck.CALCULATE
            else:
                return CalculationDependencyCheck.IGNORE
        elif depend_on in pushed_back_list:
            return CalculationDependencyCheck.IGNORE
        else:
            return CalculationDependencyCheck.PUSH_BACK
