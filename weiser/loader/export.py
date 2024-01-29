from weiser.drivers.metric_stores import MetricStoreDB

def export_results(run_id: str, metric_store: MetricStoreDB):
    metric_store.export_results(run_id)
    return True