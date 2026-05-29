for f in ./envs/*.env; do
    echo "Loading $f"
    set -a
    source "$f"
    set +a
done