git submodule deinit -f .

GIT_VER_MIN="1.9.1"
GIT_VER=$(git --version | awk '{split($0,a," "); print a[3]}')

if [  "$GIT_VER_MIN" = "`echo -e "$GIT_VER_MIN\n$GIT_VER" | sort -d | head -n1`" ]; then
  git submodule init
  git submodule status | awk '{print $2}' | xargs -P3 -n1 git submodule update --init --depth 1 --recursive
else
  git submodule update --init --recursive
fi
