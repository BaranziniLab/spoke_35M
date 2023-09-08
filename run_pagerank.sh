GRAPH_PATH="../spoke41M_data/spoke_41M.gpickle"
MAPPING_FILE="../spoke41M_data/priority_genes.csv"
IDENTIFIER_COLUMN="identifier"
NCORES=80
bucket_name="ic-spoke"
sublocation="spoke41M/ppr_vectors/genes"
check_existing_compounds=0


conda run -n pagerank python nx_pagerank_2.py "$GRAPH_PATH" "$MAPPING_FILE" "$IDENTIFIER_COLUMN" "$NCORES" "$bucket_name" "$sublocation" "$check_existing_compounds" >> "logs/run_pagerank.log" 2>&1 &
wait
echo "Completed computing PPR vectors"