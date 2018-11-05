import pandas as pd
import networkx as nx
import pydot

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.DB.Common import Common as CommonV1

from Projects.MARSUAE.Utils.AtomicKpiCalculator import CountCalculation, AggregationCalculation, \
    DistributionCalculation, LinearSOSCalculation, AvailabilityCalculation


class Results(object):
    def __init__(self, data_provider):
        self._data_provider = data_provider
        self.kpi_sheets = self._data_provider.kpi_sheets
        self.common = CommonV2(self._data_provider)
        self.common_v1 = CommonV1(self._data_provider)
        self.kpi_results = pd.DataFrame(columns=['kpi_name', 'fk', 'score'])

    def calculate(self, hierarchy):
        for level_1 in hierarchy['Level_1']:
            kpi_level_1_fk = self.get_kpi_fk(level_1)
            sum_level_1_result = 0
            parent_level_1_identifier = self.common.get_dictionary(kpi_fk=kpi_level_1_fk)

            for level_2 in hierarchy['Level_2']:
                kpi_level_2_fk = self.get_kpi_fk(level_2)
                sum_level_2_result = 0
                parent_level_2_identifier = self.common.get_dictionary(kpi_fk=kpi_level_2_fk)
                for level_3 in hierarchy['Level_3']:
                    kpi_level_3_fk = self.get_kpi_fk(level_3)
                    result = self._get_atomic_result(level_3, kpi_level_3_fk)
                    result = result.to_dict
                    result.update({'identifier_parent': parent_level_2_identifier})
                    self.common.write_to_db_result(**result)
                    self.common_v1.write_to_db_result(score=int(result['score']), result=float(result['score']),
                                                      result_2=float(result['target']), level=3, fk=k[kpi_level_3_fk])
                    sum_level_2_result += result['score']
                calculation = self._kpi_type_calculator_mapping['Aggregation'](self._data_provider, kpi_level_2_fk)
                level_2_result = calculation.calculate(**{'score': sum_level_2_result})
                result = level_2_result.to_dict
                result.update({'identifier_parent': parent_level_1_identifier})
                self.common.write_to_db_result(**result)
                self.common_v1.write_to_db_result(score=int(result['score']), result=float(result['score']),
                                                  result_2=float(result['target']), level=3, fk=k[kpi_level_2_fk])
                sum_level_1_result += result['score']

            calculation = self._kpi_type_calculator_mapping['Aggregation'](self._data_provider, kpi_level_1_fk)
            level_2_result = calculation.calculate(**{'score': sum_level_1_result})
            result = level_2_result.to_dict
            self.common.write_to_db_result(**result)
            self.common_v1.write_to_db_result(score=int(result['score']), result=float(result['score']),
                                              result_2=float(result['target']), level=3, fk=k[kpi_level_1_fk])

        # dependencies_graph = self.build_dependencies_graph(hierarchy)
        # kpi_list = self.build_kpi_list_from_dependencies_graph(dependencies_graph)
        # self.recursive_kpi_calculate(kpi_list, dependencies_graph)

    def get_kpi_fk(self, kpi_name):
        return self.common.get_kpi_fk_by_kpi_type(kpi_name)

    @staticmethod
    def _create_atomic_result(atomic_kpi_name, kpi_name, kpi_set_name, result, score=None, threshold=None,
                              weight=None):
        return {'result': result,
                'atomic_kpi_name': atomic_kpi_name,
                'kpi_name': kpi_name,
                'atomic': kpi_set_name,
                'score': score,
                'weight': weight}

    def _get_atomic_result(self, atomic, kpi_fk):
        kpi_params = self.get_kpi_params(atomic)
        calculation = self._kpi_type_calculator_mapping[kpi_params['KPI Type'].iloc[0]](self._data_provider, kpi_fk)
        return calculation.calculate(kpi_params)

    def get_kpi_params(self, atomic):
        kpi_df = self.kpi_sheets[atomic[1]]
        return kpi_df[kpi_df['Atomic KPI'] == atomic[0]]

    @property
    def _kpi_type_calculator_mapping(self):
        return {
            DistributionCalculation.kpi_type: DistributionCalculation,
            CountCalculation.kpi_type: CountCalculation,
            LinearSOSCalculation.kpi_type: LinearSOSCalculation,
            AvailabilityCalculation.kpi_type: AvailabilityCalculation,
            AggregationCalculation.kpi_type: AggregationCalculation
        }

    # def recursive_kpi_calculate(self, kpi_list, dependencies_graph):
    #     if kpi_list:
    #         kpi = kpi_list[0]
    #         kpi_neighbors = nx.neighbors(dependencies_graph, kpi[0])
    #         if kpi_neighbors:
    #             kpi_list = self.recursive_kpi_calculate(kpi_list[1:], dependencies_graph)
    #             relevant_kpis = self.kpi_results[self.kpi_results['kpi_name'].isin(kpi_neighbors)]
    #             results = sum(relevant_kpis['score'])
    #         else:
    #             results = self._get_atomic_result(kpi)
    #         for result in results:
    #             result = result.to_dict
    #             self._data_provider.common.write_to_db_result(**result)
    #             self.kpi_results = self.kpi_results.append(pd.Series(
    #                 {'kpi_name': kpi[0], 'fk': result['fk'], 'score': result['score']}),
    #                 ignore_index=True)
    #
    # def _get_set_result(self, kpi_results):
    #     set_results = {}
    #     for kpi in kpi_results:
    #         set_results[kpi['set']] = set_results.setdefault(kpi['set'], float(0)) + float(kpi['score'])
    #     for set_, score in set_results.iteritems():
    #         self._writer.write_to_db_level_1_result(set_, score)
    #
    # def _get_kpi_results(self, atomic_results):
    #     results = []
    #     for kpi in atomic_results:
    #         atomic_results[kpi]['weight'].fillna(1, inplace=True)
    #         atomic_results[kpi]['score'] = atomic_results[kpi]['result']
    #         kpi_score = atomic_results[kpi].groupby(['kpi', 'set'], as_index=False).agg(
    #             {'score': 'mean', 'weight': 'sum'})
    #         score = kpi_score['score'].iloc[0]
    #         weight = kpi_score['weight'].iloc[0]
    #         set_ = kpi_score['set'].iloc[0]
    #         results.append({'score': score, 'set': set_, 'kpi': kpi, 'weight': weight})
    #         self._writer.write_to_db_level_2_result(kpi, set_, score, weight)
    #     return results
    #
    # @staticmethod
    # def build_dependencies_graph(hierarchy):
    #     kpi_mapping = []
    #     level_hierarchy = hierarchy.copy()
    #     for column in hierarchy.columns:
    #         if ('Level' not in column) or ('Level' in column and 'type' in column):
    #             level_hierarchy.pop(column)
    #     dependencies_graph = nx.DiGraph()
    #     graph = pydot.Dot(graph_type='digraph')
    #
    #     columns = list(reversed(level_hierarchy.columns))
    #     for i, level_row in hierarchy.iterrows():
    #         dependents = [
    #             {'kpi_name': level_row[l1], 'depends_on': level_row[l2], 'kpi_type': level_row.get(l1 + '_type')} for
    #             l1, l2 in zip(columns, columns[1:])]
    #         dependents.append({'kpi_name': level_row['Level_1']})
    #         for kpi in dependents:
    #             if kpi not in kpi_mapping:
    #                 kpi_mapping.append(kpi)
    #     for kpi in kpi_mapping:
    #         dependencies_graph.add_node(kpi['kpi_name'], kpi_type=kpi.get('kpi_type'), depends_on=kpi.get('depends_on'))
    #         graph.add_node(pydot.Node(kpi['kpi_name']))
    #         if kpi.get('depends_on'):
    #             dependencies_graph.add_edge(kpi['depends_on'], kpi['kpi_name'])
    #             graph.add_edge(pydot.Edge(kpi['depends_on'], kpi['kpi_name']))
    #     # nx.draw(self.dependencies_graph)
    #     # graph.write_png('/home/israels/Desktop/example1_graph.png')
    #     return dependencies_graph
    #
    # @staticmethod
    # def build_kpi_list_from_dependencies_graph(dependencies_graph):
    #     kpi_nodes = dependencies_graph.node
    #     kpi_topological_sort = nx.topological_sort(dependencies_graph)
    #     kpi_list = [(kpi, kpi_nodes[kpi]['kpi_type']) for kpi in kpi_topological_sort]
    #     # kpi_list.reverse()
    #     return kpi_list
