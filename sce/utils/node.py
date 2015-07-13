def cluster_name(node):
    return node.parent_cluster.name.replace('@sc-', '')


def node_str_to_node(cluster, node_str):
    if node_str == 'ALL':
        return cluster.nodes
    else:
        return filter(lambda n: n.alias == node_str, cluster.nodes)[0]