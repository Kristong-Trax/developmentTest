import pandas as pd


class Node(object):
    def __init__(self, initial_df, data=None, next_node=None):
        self.data = data
        self.next_node = next_node
        self.initial_df = initial_df

    def get_data(self):
        return self.data

    def get_next(self):
        return self.next_node

    def set_next(self):
        new_d = self.initial_df[self.initial_df['kpi_type'] == self.data]['kpi_parent'].values[0]
        self.next_node = new_d

    @staticmethod
    def get_kpi_execute_list(store_atomics):
        kpis_out_of_hierarchy = store_atomics[(store_atomics['kpi_child'].isnull()) &
                                              (store_atomics['kpi_parent'].isnull())]

        execute_list = kpis_out_of_hierarchy['kpi_type'].values.tolist()
        initial_df = store_atomics[['kpi_type', 'kpi_parent', 'kpi_child']]
        initial_df = initial_df[~(initial_df['kpi_type'].isin(execute_list))]
        # input_df['kpi_parent'] = input_df['kpi_parent'].apply(lambda x: x if x else None)
        # input_df['kpi_child'] = input_df['kpi_child'].apply(lambda x: x if x else None)
        if not initial_df.empty:
            start_df = initial_df[initial_df['kpi_child'].isnull()]
            threads_dict = dict()
            for i, row in start_df.iterrows():
                threads_dict.update({i: []})
                new_node = Node(initial_df, row['kpi_type'])
                new_node.set_next()
                threads_dict[i].append(new_node)
                flag = True
                while flag:
                    new_node = Node(initial_df, new_node.next_node)
                    new_node.set_next()
                    threads_dict[i].append(new_node)
                    if new_node.next_node is None:
                        flag = False
            for key, nodes_list in threads_dict.items():
                nodes_list.reverse()

            max_len = max(map(lambda x: len(x), threads_dict.values()))

            # execute_list = []
            for i in range(max_len - 1, -1, -1):
                for key, nodes_list in threads_dict.items():
                    if i <= len(nodes_list) - 1:
                        if nodes_list[i].data not in execute_list:
                            execute_list.append(nodes_list[i].data)

        return execute_list


class ReorderKpisMethods(object):
    PARENT_KPI = 'kpi_parent'
    CHILD_KPI = 'kpi_child'
    KPI_TYPE = 'kpi_type'

    def _reorder_kpis(self, store_atomics):
        input_df = store_atomics.copy()
        input_df['remaining_child'] = input_df[self.CHILD_KPI].copy()
        input_df.loc[~(input_df['remaining_child'] == ''), 'remaining_child'] = input_df['remaining_child'].apply(
            lambda x: x if isinstance(x, list) else [x])
        reordered_df = pd.DataFrame(columns=input_df.columns.values.tolist())
        child_flag = True
        while child_flag:
            input_df['remaining_child'] = input_df.apply(self._check_remaining_child, axis=1, args=(input_df,))
            remaining_df = input_df[(input_df['remaining_child'] == '') |
                                    (input_df['remaining_child'].isnull())]
            reordered_df = reordered_df.append(remaining_df)
            input_df = input_df[(~(input_df['remaining_child'] == '')) &
                                (~(input_df['remaining_child'].isnull()))]
            if input_df.empty:
                child_flag = False
        reordered_index = reordered_df.index
        return reordered_index

    def _check_remaining_child(self, row, initial_df):
        child_kpis = row['remaining_child']
        if child_kpis:
            child_kpis = child_kpis if isinstance(child_kpis, (list, tuple)) else [child_kpis]
            for kpi in child_kpis:
                if kpi not in initial_df[self.KPI_TYPE].values:
                    ind_to_remove = [i for i, x in enumerate(child_kpis) if x == kpi]
                    for ind in ind_to_remove:
                        child_kpis.pop(ind)
                        if len(child_kpis) == 0:
                            child_kpis = ''
        return child_kpis

    def _restore_children(self, store_atomics):
        parents_list = store_atomics[self.PARENT_KPI].unique().tolist()
        parents_list = filter(lambda x: x, parents_list)
        for parent in parents_list:
            child_kpis = store_atomics[store_atomics[self.PARENT_KPI] == parent][self.KPI_TYPE].unique().tolist()
            store_atomics[self.CHILD_KPI] = store_atomics[self.CHILD_KPI].astype(object)
            store_atomics.loc[store_atomics[self.KPI_TYPE] == parent, self.CHILD_KPI] = [','.join(child_kpis)]
            store_atomics[self.CHILD_KPI] = store_atomics[self.CHILD_KPI].apply(lambda x: x.split(',') if x else '')

    def get_correct_atomic_sequence_index(self, store_atomics):
        reordered_index = self._reorder_kpis(store_atomics)
        self._restore_children(store_atomics)
        return reordered_index
