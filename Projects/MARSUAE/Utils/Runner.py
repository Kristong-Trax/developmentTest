import pandas as pd
import numpy as np

from Projects.MARSUAE.Utils.AtomicKpiCalculator import AvailabilityFacingCalculation, \
    AvailabilityHangingStripCalculation, CountCalculation, DistributionCalculation, SOSCalculation


class Results(object):
    def __init__(self, data_provider):
        self._data_provider = data_provider

    def calculate_old_tables(self, hierarchy):
        atomic_results = self._get_atomic_results(hierarchy)
        kpi_results = self._get_kpi_results(atomic_results)
        set_result = self._get_set_result(kpi_results)

    def calculate(self, hierarchy):
        level_hierarchy = hierarchy.copy()
        for column in hierarchy.columns:
            if 'Level' not in column:
                level_hierarchy.pop(column)
        import networkx as nx
        self.dependencies_graph = nx.DiGraph()
        self.kpi_mapping = []
        for i, level_row in level_hierarchy.iterrows():
            columns = list(reversed(level_row.index))
            dependents = [{'kpi_type': level_row[l1], 'depends_on': level_row[l2]} for l1, l2 in zip(columns, columns[1:])]
            dependents.append({'kpi_type': level_row['Level_1'], 'depends_on': []})
            self.kpi_mapping.append(dependents)
        for calculation in self.kpi_mapping:
            for kpi in calculation:
                self.dependencies_graph.add_node(kpi['kpi_type'], calculation=kpi)
        for calculation in self.kpi_mapping:
            # for dependency in calculation.depends_on:
            for kpi in calculation:
                if kpi['depends_on']:
                    self.dependencies_graph.add_edge(kpi['depends_on'], kpi['kpi_type'])
        nx.draw(self.dependencies_graph)

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
        for atomic in atomics:
            calculation = self._kpi_type_calculator_mapping[atomic['kpi_type']](self._data_provider)
            result = {'result': calculation.calculate(atomic),
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
            DistributionCalculation.kpi_type: DistributionCalculation,
            CountCalculation.kpi_type: CountCalculation,
            AvailabilityHangingStripCalculation.kpi_type: AvailabilityHangingStripCalculation,
            AvailabilityFacingCalculation.kpi_type: AvailabilityFacingCalculation,
            SOSCalculation.kpi_type: SOSCalculation
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


