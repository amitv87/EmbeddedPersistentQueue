set -e
# set -x

NUM_CPU=$(getconf _NPROCESSORS_ONLN)

ROOT_DIR=$(pwd)

cd $ROOT_DIR/deps/junction
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DJUNCTION_WITH_SAMPLES=FALSE
make -j $NUM_CPU

cd $ROOT_DIR
