N_JOBS=16
FORMAT="flac"
SAMPLE_RATE=24000
COOKIE_PATH="cookies.txt"

ROOT_PATH="unbalanced_train_segments"

FINAL_PATH="seg_00"
METADATA_PATH="csvs/audioset_unbalanced_splits/unbalanced_train_segments_00.csv"

python3 download.py \
    --root-path=$ROOT_PATH \
    --final-path=$FINAL_PATH \
    --metadata-path=$METADATA_PATH \
    --n-jobs=$N_JOBS \
    --format=$FORMAT \
    --sample-rate=$SAMPLE_RATE \
    --cookie-path=$COOKIE_PATH