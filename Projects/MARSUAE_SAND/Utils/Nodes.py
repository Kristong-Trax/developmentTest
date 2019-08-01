
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
