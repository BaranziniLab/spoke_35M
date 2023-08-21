GRAPH_PATH = "../ncats/data/spoke_graph_ncats_version_2021_02_07.gpickle"
MAPPING_FILE = "../ncats/data/genes.csv"
IDENTIFIER_COLUMN = "identifier"
NCORES = 80
bucket_name = "ic-spoke"
sublocation = "spoke35M/ncats/ppr_vectors"
check_existing_compounds = 0


conda run -n pagerank python nx_pagerank_2.py "$GRAPH_PATH" "$MAPPING_FILE" "$IDENTIFIER_COLUMN" "$NCORES" "$bucket_name" "$sublocation" "$check_existing_compounds" >> "logs/run_script.log" 2>&1 &
wait
echo "Completed computing PPR vectors"