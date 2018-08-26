import pandas as pd
import numpy as np
import networkx as nx
import pydot

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
            if ('Level' not in column) or ('Level' in column and 'type' in column):
                level_hierarchy.pop(column)
        self.dependencies_graph = nx.DiGraph()
        graph = pydot.Dot(graph_type='digraph')

        self.kpi_mapping = []
        columns = list(reversed(level_hierarchy.columns))
        for i, level_row in hierarchy.iterrows():
            dependents = [{'kpi_name': level_row[l1], 'depends_on': level_row[l2], 'kpi_type': level_row.get(l1 + '_type')} for l1, l2 in zip(columns, columns[1:])]
            dependents.append({'kpi_name': level_row['Level_1']})
            for kpi in dependents:
                if kpi not in self.kpi_mapping:
                    self.kpi_mapping.append(kpi)
        for kpi in self.kpi_mapping:
            self.dependencies_graph.add_node(kpi['kpi_name'], kpi_type=kpi.get('kpi_type'), depends_on=kpi.get('depends_on'))
            graph.add_node(pydot.Node(kpi['kpi_name']))
            if kpi.get('depends_on'):
                self.dependencies_graph.add_edge(kpi['depends_on'], kpi['kpi_name'])
                graph.add_edge(pydot.Edge(kpi['depends_on'], kpi['kpi_name']))
        nx.draw(self.dependencies_graph)
        graph.write_png('/home/israels/Desktop/example1_graph.png')

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


