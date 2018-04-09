git submodule deinit -f .
git submodule init
git submodule status | awk '{print $2}' | xargs -P3 -n1 git submodule update --depth 1 --recursive
