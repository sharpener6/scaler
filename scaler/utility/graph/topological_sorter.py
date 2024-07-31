import logging

try:
    from scaler.utility.graph.topological_sorter_graphblas import TopologicalSorter

    logging.info("using GraphBLAS for calculate graph")
except ImportError as e:
    assert isinstance(e, Exception)
    from graphlib import TopologicalSorter

    assert isinstance(TopologicalSorter, object)