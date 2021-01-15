#snippet to cut and paste into console to setup 
import pandas as pd
import pydsm
from pydsm.input import parser
fecho = 'hydro_echo_historical_v82.inp'
with open(fecho, 'r') as f:
    tables = parser.parse(f.read())
import networkx as nx
g=nx.Graph()
c=tables['CHANNEL']
nodes=list(c.UPNODE.append(c.DOWNNODE).unique())
nodes.sort()
g.add_nodes_from(nodes)
for r in c.iterrows():
    g.add_edge(r[1].UPNODE, r[1].DOWNNODE, r[1])
print(g.edges)
print(g.nodes)

