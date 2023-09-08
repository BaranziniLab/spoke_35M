import networkx as nx
import sys
import pickle
import time
import joblib


GRAPH_PATH = sys.argv[1]
max_iter = int(sys.argv[2])
tol = float(sys.argv[3])
NODETYPE_SEPARATOR = sys.argv[4]

node_list = ['Gene'+NODETYPE_SEPARATOR+'2']
max_iter = [200, 300, 400, 500]
tol = [1e-20, 1e-25, 1e-30, 1e-35]

N = 10


def main():
    with open(GRAPH_PATH, "rb") as f:
        G = pickle.load(f)

    completion_time_list = []
    item = node_list[0]
    personalization = {item:1}
    start_time = time.time()
    personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization, max_iter=max_iter, tol=tol)
    completion_time = round((time.time()-start_time)/(60),2)
    print("PPR is completed in {} min".format(completion_time))
    
    filtered_dict = {k:v for k,v in personalized_pagerank.items() if "Compound:inchikey" in k}
    top_N_compounds = sorted(filtered_dict.items(), key=lambda x: x[1], reverse=True)[:N]
    top_N_compounds_ids = [x[0] for x in top_N_compounds]
    compound_neighbors = [n for n in G.neighbors(node_list[0]) if n.startswith('Compound:inchikey')]
    top_compound_neighbors = set(top_N_compounds_ids).intersection(set(compound_neighbors))
    print("Out of {} top compounds in PPR vector, {} compounds are first nbrs of the selected node for which PPR is computed".format(N, len(top_compound_neighbors)))

            
    


if __name__ == "__main__":
    main()