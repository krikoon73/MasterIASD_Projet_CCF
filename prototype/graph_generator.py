# Simple tool for generating graph
import networkx as nx
from networkx.generators.random_graphs import erdos_renyi_graph
import matplotlib.pyplot as plt

# Example 1 : simple random graph
n = 10
p = 0.5
g = erdos_renyi_graph(n, p,seed=10,directed=False)
#print(g.nodes)
#print(g.edges)
g1=open("simple_random_graph.csv",'wb')
nx.write_edgelist(g, g1,delimiter=",",data=False)

# Example 2 : 2 distincts sub-graph
G = nx.Graph()
G.add_edges_from([(1, 2), (1, 3), (2, 3), (2, 4), (2, 5), (3, 4),(4, 5), (4, 6), (5, 7), (5, 8), (7, 8)])
#plt.subplot(311) 
#nx.draw_networkx(G) 

H = nx.Graph() 
H.add_edges_from([(13, 14), (13, 15), (13, 9),(14, 15), (15, 10), (9, 10)])
#plt.subplot(312) 
#nx.draw_networkx(H)

I = nx.union(G, H) 
#plt.subplot(313) 
#nx.draw_networkx(I)

g2=open("simple_2_graphs.csv",'wb')
nx.write_edgelist(I, g2,delimiter=",",data=False)