LOCAL_PORT=""
if [ ! -z "$1" ]
  then
      LOCAL_PORT=$(cat $1)
      echo "Running reverse tunnel on port $LOCAL_PORT"
fi

tmux new -d -s reverse_tunnel
tmux send-keys -t reverse_tunnel.0 "cd $(pwd)/scripts; ./setup_reverse_tunnel.sh $LOCAL_PORT" ENTER
