NUMBER_OF_FEATURES=256
MAPPING_FILE="../spoke41M_data/priority_genes.csv"
IDENTIFIER_COLUMN="identifier"
BUCKET_NAME="ic-spoke"
SUBLOCATION="spoke41M/ppr_vectors/genes"
PPR_FEATURE_LOCATION="spoke41M/ppr_vectors/spoke41M_ppr_features.csv"
SAVE_PATH="../spoke41M_data"
SAVE_FILENAME="priority_gene_vectors_reduced_dim.h5"
CHECK_ANY_SAVED_NODES=0
NODE_TYPE="Gene"
NODE_TYPE_SEPERATOR="|"

start_time=$(date +%s)
conda run -n pagerank python rank_normalized_feature_reduction.py "$NUMBER_OF_FEATURES" "$MAPPING_FILE" "$IDENTIFIER_COLUMN" "$BUCKET_NAME" "$SUBLOCATION" "$PPR_FEATURE_LOCATION" "$SAVE_PATH" "$SAVE_FILENAME" "$CHECK_ANY_SAVED_NODES" "$NODE_TYPE" "NODE_TYPE_SEPERATOR" >> logs/rank_normalized_feature_reduction.log 2>&1 &
wait
end_time=$(date +%s)
time_taken_hours=$(( (end_time - start_time) / 3600 ))
echo "Completed feature reduction using rank normalization"
echo "Time taken to complete : $time_taken_hours hours"