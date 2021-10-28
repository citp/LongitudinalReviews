HOME_IP=$(python -m crawler.configuration controller hostname)
HOME_USER="ubuntu"
HOME_PORT="22"
LOCAL_PORT="43022"

if [ ! -z "$1" ]
  then
    LOCAL_PORT="$1"
fi

while true; do
    echo "Attempting to connect"
    ssh -o ServerAliveInterval=15 -N -R "$LOCAL_PORT":localhost:22 -p "$HOME_PORT" "$HOME_USER"@"$HOME_IP"
    sleep 15s
done

      
