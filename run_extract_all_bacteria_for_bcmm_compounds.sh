GRAPH_PATH="../spoke_35M_data/spoke_35M.gpickle"
MAPPING_FILE="../bcmm_data/compounds_with_experimental_associations_from_peter.csv"
BACTERIA_FILE="../spoke_35M_data/spoke_bacteria_nodes.tsv"
BUCKET_NAME="ic-spoke"
PPR_FILE_LOCATION="spoke35M/spoke35M_compound_pruned_version_converged_ppr/ppr_with_bcmm_experimental_associations"
PPR_FEATURE_MAP_FILENAME="spoke35M_ppr_features.csv"
SAVE_LOCATION="spoke35M/bcmm_data"
SAVE_NAME="all_bateria_values_for_bcmm_compounds_with_experimental_associations_from_peter.pickle"
NCORES=4
NODE_TYPE_SEPARATOR=":"


start_time=$(date +%s)
conda run -n pagerank python extract_all_bacteria_for_bcmm_compounds.py "$GRAPH_PATH" "$MAPPING_FILE" "$BACTERIA_FILE" "$BUCKET_NAME" "$PPR_FILE_LOCATION" "$PPR_FEATURE_MAP_FILENAME" "$SAVE_LOCATION" "$SAVE_NAME" "$NCORES" "$NODE_TYPE_SEPARATOR"
wait
end_time=$(date +%s)
time_taken_hours=$(( (end_time - start_time) / 3600 ))
echo "Completed extraction of all bacteria associated with bcmm compounds"
echo "Time taken to complete : $time_taken_hours hours"