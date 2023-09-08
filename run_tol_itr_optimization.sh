GRAPH_PATH="../spoke41M_data/spoke_41M.gpickle"
max_iter=200
tol=1e-12
NODETYPE_SEPARATOR="|"


conda run -n pagerank python nx_pagerank_tolerance_optimization.py "$GRAPH_PATH" "$max_iter" "$tol" "$NODETYPE_SEPARATOR" >> "logs/tol_opt_41M_18.log" 2>&1 &
wait
echo "Completed computing PPR vectors"