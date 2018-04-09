set -e
BINS="*epq*"
ROOT_DIR=$(pwd)
OUT_DIR=.out
mkdir -p $OUT_DIR
cd $OUT_DIR
# rm -rf $BINS
cmake ..
make -j8

ls -lah $BINS

cd ..

.out/epq_test
