
__author__ = 'Dudi S'


def get_accumulative_scoring_by_score_groups(atomic_kpi_results):
    kpi_score = 0
    scoring_check_point = 0
    filtered_results = _filter_result_to_include_in_scoring(atomic_kpi_results)
    if not filtered_results.empty:
        filtered_results.loc[:, 'score_target'] = 100
        results_score_per_group = filtered_results.groupby('score_group', as_index=False)[
            'score', 'score_target'].sum()
        results_score_per_group.sort_values('score_group', ascending=True)
        for ind, group in results_score_per_group.iterrows():
            if group['score'] == group['score_target']:
                kpi_score += (group['score_group'] - scoring_check_point)
                scoring_check_point = group['score_group']
            else:
                break
    return kpi_score


def get_binary_add_scoring(atomic_kpi_results):
    kpi_score = 0
    filtered_results = _filter_result_to_include_in_scoring(atomic_kpi_results)
    if not filtered_results.empty:
        filtered_results.loc[:, 'score_target'] = 100
        total_target = filtered_results['score_target'].sum()
        total_score = filtered_results['score'].sum()
        if 'extra_scores_for_missing_prods' in filtered_results.keys():
            missing_prods_score = filtered_results['extra_scores_for_missing_prods'].sum()
            if total_score > 0:
                kpi_score = 100
            if (missing_prods_score + total_score) == total_target:
                kpi_score = 100
            if missing_prods_score == total_target or filtered_results['failed'].sum() > 0:
                kpi_score = 0
        elif total_score == total_target:
            kpi_score = 100
    return kpi_score


def get_proportional_scoring(atomic_kpi_results):
    kpi_score = 0
    filtered_results = _filter_result_to_include_in_scoring(atomic_kpi_results)
    if not filtered_results.empty:
        filtered_results.loc[:, 'score_target'] = 100
        total_target = filtered_results['score_target'].sum()
        total_score = filtered_results['score'].sum()
        kpi_score = total_score / total_target * 100
    return kpi_score


def get_proportional_scoring_by_groups(atomic_kpi_results):
    kpi_score = 0
    filtered_results = _filter_result_to_include_in_scoring(atomic_kpi_results)
    if not filtered_results.empty:
        filtered_results.loc[:, 'score_target'] = 100
        score_per_group = filtered_results.groupby(['score_group', 'group_weight'], as_index=False)[
            'score', 'score_target'].sum().rename(columns={'score': 'total_score', 'score_target': 'total_score_target'})
        for ind, group in score_per_group.iterrows():
            kpi_score += group['total_score'] / group['total_score_target'] * group['group_weight']
    return kpi_score


def _filter_result_to_include_in_scoring(atomic_kpi_results):
    return atomic_kpi_results.loc[atomic_kpi_results['include_in_scoring'] != 'No']
