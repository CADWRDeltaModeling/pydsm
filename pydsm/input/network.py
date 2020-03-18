# Creates a network containing channel and reservoir connections as nodes and channels and reservoirs as arcs.
import networkx as nx


def build_network_channels(tables):
    '''
    Builds network with channel upnodes -> downnodes. Stores the columns as attributes of each node
    '''
    c = tables['CHANNEL']
    gc = nx.from_pandas_edgelist(c, source='UPNODE', target='DOWNNODE', edge_attr=list(
        c.columns), create_using=nx.MultiDiGraph)
    return gc


def build_network_reservoirs(tables):
    '''
    Builds network of each reservoir and connected nodes. Uses a directed graph and creates two edges for
    each resevoir connection, one from reservoir to node and from node to the reservoir.
    In this way, each reservoir is treated as a node in the graph
    '''
    rc = tables['RESERVOIR_CONNECTION']
    grc1 = nx.from_pandas_edgelist(rc, source='RES_NAME', target='NODE', edge_attr=list(
        rc.columns), create_using=nx.MultiDiGraph)
    grc2 = nx.from_pandas_edgelist(rc, source='NODE', target='RES_NAME', edge_attr=list(
        rc.columns), create_using=nx.MultiDiGraph)
    return nx.compose(grc1, grc2)


def build_network(tables):
    '''
    Builds a graph network of DSM2 nodes using the information from input files loaded as pandas DataFrames
    Use the parser to load input file (echo files) as pandas DataFrame to call this function
    returns a nx.MultiDiGraph of nodes/reservoirs linked to channels/gates
    '''
    gc = build_network_channels(tables)
    grc = build_network_reservoirs(tables)
    g = nx.compose(gc, grc)
    return g
