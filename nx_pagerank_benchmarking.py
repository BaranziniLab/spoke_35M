import networkx as nx
import sys
import pickle
import time
import joblib


GRAPH_PATH = sys.argv[1]
SAVE_PATH = sys.argv[2]

node_list = ['Compound:inchikey:QPIJOYAYKONXJD-ZYLBBPIUSA-N', 'Compound:inchikey:HBYRDDGQNQMGCZ-QMMMGPOBSA-N', 'Compound:inchikey:WISJHPKKVVBBCE-QGZVFWFLSA-N', 'Compound:inchikey:POJGLMCWEBQVGJ-QFIPXVFZSA-N', 'Compound:inchikey:DXWQLTOXWVWMOH-UHFFFAOYSA-N', 'Compound:inchikey:BRKIAEBRDYHMFE-KKQCDPINSA-N', 'Compound:inchikey:UWVIZLOHFWAKKC-MNGKLMTLSA-N', 'Compound:inchikey:YNTUOLUMNOOQFL-UHFFFAOYSA-N', 'Compound:CHEBI:143130', 'Compound:inchikey:LILZBQJGXXRVSP-AWEZNQCLSA-N', 'Compound:inchikey:OYFJDPJIGCMKKI-LADGPHEKSA-N', 'Compound:inchikey:HEFYJBZEIVFECQ-LJAQVGFWSA-N', 'Compound:inchikey:IPDUBURODPGKPZ-DGVOKRSTSA-O', 'Compound:inchikey:SEUMVDPBYULKQH-UHFFFAOYSA-N', 'Compound:inchikey:LIOKNOIJMJKVCG-RDSVHMIISA-N', 'Compound:inchikey:PEFWQSMSSURTDP-UHFFFAOYSA-N', 'Compound:inchikey:OTXANOLOOUNVSR-UHFFFAOYSA-N', 'Compound:inchikey:OZQPALZNLAIXSQ-SJLPKXTDSA-N', 'Compound:inchikey:UVSIBVLSEPOIBQ-UHFFFAOYSA-N']


def main():
	with open(GRAPH_PATH, "rb") as f:
	    G = pickle.load(f)

	completion_time_list = []
	for item in node_list:	
		personalization = {item:1}
		start_time = time.time()
		personalized_pagerank = nx.pagerank(G, alpha=0.85, personalization=personalization)
		completion_time_list.append(round((time.time()-start_time)/(60*60),2))
	joblib.dump(completion_time_list, SAVE_PATH)


if __name__ == "__main__":
    main()