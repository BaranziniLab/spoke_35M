GRAPH_PATH="../spoke41M_data/spoke_41M.gpickle"
BUCKET_NAME="ic-spoke"
FILE_LOCATION="spoke41M/ppr_vectors"
SAVE_NAME="spoke41M_ppr_features.csv"
NODE_TYPE_SEPERATOR="|"

start_time=$(date +%s)
conda run -n pagerank python get_pagerank_features.py "$GRAPH_PATH" "$BUCKET_NAME" "$FILE_LOCATION" "$SAVE_NAME" "$NODE_TYPE_SEPERATOR" >> logs/run_get_pagerank_features.log 2>&1 &
wait
end_time=$(date +%s)
time_taken_hours=$(( (end_time - start_time) / 3600 ))
echo "Completed the estimation of ppr features"
echo "Time taken to complete : $time_taken_hours hours"