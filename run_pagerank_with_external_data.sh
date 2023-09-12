GRAPH_PATH="../spoke_35M_data/spoke_35M_compound_pruned_version.gpickle"
EXTERNAL_DATA_PATH="../bcmm_data/bcmm_experimental_associations_from_peter.json"
MAPPING_FILE="../bcmm_data/compounds_with_experimental_associations_from_peter.csv"
IDENTIFIER_COLUMN="spoke_identifier"
EXTERNAL_DATA_COEFF=0.5
NCORES=4
bucket_name="ic-spoke"
sublocation="spoke35M/spoke35M_compound_pruned_version_converged_ppr/ppr_with_bcmm_experimental_associations"
check_any_saved_nodes=0
NODE_TYPE="Compound"
NODE_TYPE_SEPERATOR=":"

start_time=$(date +%s)
conda run -n pagerank python nx_pagerank_with_external_data.py "$GRAPH_PATH" "$EXTERNAL_DATA_PATH" "$MAPPING_FILE" "$IDENTIFIER_COLUMN" "$EXTERNAL_DATA_COEFF" "$NCORES" "$bucket_name" "$sublocation" "$check_any_saved_nodes" "$NODE_TYPE" "$NODE_TYPE_SEPERATOR" >> "logs/run_pagerank_with_external_data.log" 2>&1 &
wait
end_time=$(date +%s)
time_taken_hours=$(( (end_time - start_time) / 3600 ))
echo "Completed computing PPR vectors with the integration of external data"
echo "Time taken to complete : $time_taken_hours hours"