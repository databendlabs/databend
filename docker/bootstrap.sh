/fuse-store &
P1=$!
/fuse-query -c fuse-query.toml &
P2=$!
wait $P1 $P2
