import pandas as pd
# import numpy as np
import networkx as nx
import pydot

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from Projects.MARSUAE.Utils.AtomicKpiCalculator import AvailabilityHangingStripCalculation, CountCalculation, \
                                                       DistributionCalculation, LinearSOSCalculation


class Results(object):
    def __init__(self, data_provider):
        self._data_provider = data_provider
        self.kpi_sheets = self._data_provider.kpi_sheets
        self.common = CommonV2(self._data_provider)
        self.kpi_results = pd.DataFrame(columns=['kpi_name', 'fk', 'score'])

    def calculate_old_tables(self, hierarchy):
        atomic_results = self._get_atomic_result(hierarchy)
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
        # nx.draw(self.dependencies_graph)
        # graph.write_png('/home/israels/Desktop/example1_graph.png')

        kpi_nodes = self.dependencies_graph.node
        kpi_topological_sort = nx.topological_sort(self.dependencies_graph)
        kpi_list = [(kpi, kpi_nodes[kpi]['kpi_type']) for kpi in kpi_topological_sort]
        kpi_list.reverse()
        for kpi in kpi_list:
            kpi_neighbors = nx.neighbors(self.dependencies_graph, kpi[0])
            if kpi_neighbors:
                relevant_kpis = self.kpi_results[self.kpi_results['kpi_name'] in kpi_neighbors]
                result = sum(relevant_kpis['score'])
            else:
                result = self._get_atomic_result(kpi, kpi_neighbors)
            self._data_provider.common.write_to_db_result(result)
            self.kpi_results.append({'kpi_name': result['kpi_name'],
                                     'fk': result['fk'],
                                     'score': result['score']})

    def _create_atomic_result(self, atomic_kpi_name, kpi_name, kpi_set_name, result, score=None, threshold=None,
                              weight=None):
        return {'result': result,
                'atomic_kpi_name': atomic_kpi_name,
                'kpi_name': kpi_name,
                'atomic': kpi_set_name,
                'score': score,
                'weight': weight}

    def _get_atomic_result(self, atomic, kpi_neighbors):
        kpi_params = self.get_kpi_params(atomic)
        # kpi_fk = self.common.get_kpi_fk_by_kpi_type(atomic)
        kpi_fk = 0
        calculation = self._kpi_type_calculator_mapping[kpi_params['KPI Type'].iloc[0]](self._data_provider, kpi_fk)
        return calculation.calculate(kpi_params)
        # concat_results = atomic_results.setdefault(atomic['kpi'], pd.DataFrame()).append(pd.DataFrame([result]))
        # atomic_results[atomic['kpi']] = concat_results

        # if not np.isnan(result['result']):
        #     self._writer.write_to_db_level_3_result(
        #         atomic_kpi_name=result['atomic'],
        #         kpi_name=result['kpi'],
        #         kpi_set_name=result['set'],
        #         score=None,
        #         threshold=result['target'],
        #         result=result['result'],
        #         weight=result['weight'])

    @property
    def _kpi_type_calculator_mapping(self):
        return {
            DistributionCalculation.kpi_type: DistributionCalculation,
            CountCalculation.kpi_type: CountCalculation,
            AvailabilityHangingStripCalculation.kpi_type: AvailabilityHangingStripCalculation,
            LinearSOSCalculation.kpi_type: LinearSOSCalculation
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

    def get_kpi_params(self, atomic):
        kpi_df = self.kpi_sheets[atomic[1]]
        return kpi_df[kpi_df['Atomic KPI'] == atomic[0]]


