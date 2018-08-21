import abc
import os

import networkx as nx

from Trax.Algo.Calculations.OutOfTheBox.Actions.Calculators.Base import OobAction
from Trax.Algo.Calculations.OutOfTheBox.Calculations.Base import KpiCalculation
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Metaprogramming.Reflection import load_classes

__author__ = 'pavel'


class BaseMapping(object):
    def __init__(self, root_dirs):
        """
        Initializes the mapping
        :param root_dirs: The root directory of dynamically loaded entities
        :type root_dirs: list[str]
        """
        self._root_dirs = root_dirs
        self._mapping = {}
        for directory in root_dirs:
            try:
                self._load_supported_classes(directory, self.class_to_load, self.class_type_property)
            except ImportError as e:
                Log.debug('Skip loading calculations from {} because of import error: {}'.format(directory, e.message))

    def _load_supported_classes(self, root_dir, parent_class, attr):
        classes = load_classes(root_dir)
        for class_ in classes:
            if issubclass(class_, parent_class) and class_ is not parent_class and isinstance(getattr(class_, attr),
                                                                                              str):
                self._check_duplicates(class_, attr)
                self._mapping[getattr(class_, attr)] = class_

        Log.debug('Retrieved kpis mapping: {}'.format(self._mapping))

    def _check_duplicates(self, class_, instance):
        if getattr(class_, instance) not in self._mapping:
            return
        other_class = self._mapping[getattr(class_, instance)]
        if class_ != other_class:
            raise DuplicateCalculationNamesError(getattr(class_, instance), class_, other_class)

    @abc.abstractproperty
    def class_to_load(self):
        pass

    @abc.abstractproperty
    def class_type_property(self):
        pass

    def get_classes_by_type(self, type_):
        """
        Retrieves a class of out of the box KPI/Action according to the type.
        Returns None if no class for such KPI type exists
        :param type_: The kpi/action type
        :type type_: str
        """
        return self._mapping.get(type_, None)


class OOBCalculationsMapping(BaseMapping):

    @property
    def class_type_property(self):
        return 'kpi_type'

    @property
    def class_to_load(self):
        return KpiCalculation

    def __init__(self, root_dirs):
        super(OOBCalculationsMapping, self).__init__(root_dirs)
        self._supported_kpis = []
        self.kpi_mapping = {}
        self.dependencies_graph = nx.DiGraph()
        self._load_supported_kpis()

    @property
    def supported_kpis(self):
        return self._supported_kpis

    def get_calculation_by_kpi_type(self, kpi_type):
        self.get_classes_by_type(kpi_type)

    def get_kpis_to_recalculate_and_all_dependencies(self, kpi_name_list):
        """
        Receives a list of KPI names and finds 1) all the KPIs that depend on them, and should be recalculated along
        with them, and 2) all the KPIs that are prerequisites to the KPIs in (1), i.e. the KPIs in (1) depend on them.
        """
        kpis_to_recalculate = self._get_kpis_to_recalculate(kpi_name_list)
        prerequisites = self._get_prerequisite_kpis(kpis_to_recalculate)
        return kpis_to_recalculate, prerequisites

    def _load_supported_kpis(self):
        self.kpi_mapping = self._mapping
        self._supported_kpis = self._get_sorted_by_dependency_order()

    def _get_kpi_mapping(self, ):
        return {supported_kpi.kpi_type: supported_kpi for supported_kpi in self._supported_kpis}

    def _get_sorted_by_dependency_order(self):
        self._build_dependency_graph()
        calculations = self._sort_vertices(self.dependencies_graph)
        return calculations

    def _get_prerequisite_kpis(self, kpi_list):
        prerequisites_names = set()
        for kpi in kpi_list:
            prerequisites_names |= nx.ancestors(self.dependencies_graph, kpi.kpi_type)

        prerequisites = self._get_calculations_from_vertex_names(prerequisites_names)
        prerequisites -= set(kpi_list)
        return list(prerequisites)

    def _get_kpis_to_recalculate(self, kpi_name_list):
        try:
            dependant_kpis_unordered = set()
            for kpi_name in kpi_name_list:
                dependant_kpis_unordered.add(kpi_name)
                dependant_kpis_unordered |= nx.descendants(self.dependencies_graph, kpi_name)

            subgraph = self.dependencies_graph.subgraph(dependant_kpis_unordered)
            return self._sort_vertices(subgraph)
        except nx.NetworkXError as e:
            raise UnknownCalculationNameError(str(e))

    def _sort_vertices(self, graph):
        sorted_vertices = nx.topological_sort(graph)
        calculations = [graph.node[v]['calculation'] for v in sorted_vertices]
        return calculations

    def _build_dependency_graph(self):
        self._add_vertices()
        self._add_edges()
        if not nx.is_directed_acyclic_graph(self.dependencies_graph):
            raise CircularDependencyError()

    def _add_edges(self):
        for calculation in self.kpi_mapping.values():
            for dependency in calculation.depends_on:
                self.dependencies_graph.add_edge(dependency, calculation.kpi_type)

    def _add_vertices(self):
        for calculation in self.kpi_mapping.values():
            self.dependencies_graph.add_node(calculation.kpi_type, calculation=calculation)

    def _get_calculations_from_vertex_names(self, vertex_names):
        return {self.dependencies_graph.node[name]['calculation'] for name in vertex_names}


class ActionMapping(BaseMapping):

    @property
    def class_type_property(self):
        return 'action_type'

    @property
    def class_to_load(self):
        return OobAction

    def __init__(self, root_dirs):
        super(ActionMapping, self).__init__(root_dirs)
        self.action_mapping = self._mapping

    def get_action_by_type(self, action_type):
        self.get_classes_by_type(action_type)


class DuplicateCalculationNamesError(Exception):
    def __init__(self, duplicate_name, class1, class2):
        message = 'Calculations in {} and {} have the same name - {}'.format(class1, class2, duplicate_name)
        super(DuplicateCalculationNamesError, self).__init__(message)


class CircularDependencyError(Exception):
    def __init__(self):
        message = 'A circular dependency found in KPI calculation'
        super(CircularDependencyError, self).__init__(message)


class UnknownCalculationNameError(Exception):
    def __init__(self, msg):
        message = 'Calculation with this name does not exist: {}'.format(msg)
        super(UnknownCalculationNameError, self).__init__(message)
