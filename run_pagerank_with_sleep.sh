GRAPH_PATH="../spoke41M_data/spoke_41M.gpickle"
MAPPING_FILE="../spoke41M_data/all_genes.csv"
IDENTIFIER_COLUMN="identifier"
NCORES=15
bucket_name="ic-spoke"
sublocation="spoke41M/ppr_vectors/genes"
check_any_saved_nodes=1
NODE_TYPE_TO_CHECK="Gene"
NODE_TYPE_SEPERATOR="|"

start_time=$(date +%s)
total_rows=$(wc -l < "$MAPPING_FILE")

while true; do
  num_files=$(aws s3 ls "s3://$bucket_name/$sublocation/" | wc -l)
  if [ "$num_files" -eq "$total_rows" ]; then
    echo "All PPR vectors are in the S3 bucket. Exiting the loop."
    break
  else
    echo "Number of PPR vectors in S3: $num_files, Total nodes in mapping file: $total_rows. They do not match and hence continuing with PPR computation for the remaining nodes ..."
  fi
  conda run -n pagerank python nx_pagerank_2.py "$GRAPH_PATH" "$MAPPING_FILE" "$IDENTIFIER_COLUMN" "$NCORES" "$bucket_name" "$sublocation" "$check_any_saved_nodes" "$NODE_TYPE_TO_CHECK" "$NODE_TYPE_SEPERATOR" >> "logs/run_pagerank_with_sleep.log" 2>&1 &
  sleep 2h
  pkill -f "python nx_pagerank_2.py"
  sleep 60
done
end_time=$(date +%s)
time_taken_hours=$(( (end_time - start_time) / 3600 ))
echo "Time taken to complete : $time_taken_hours hours"