source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN
seed=$SEED
range_start=1
range_end=10

# Set the seed for random number generation
RANDOM=$seed

# Generate the random number permutation array
perm_array=($(seq $range_start $range_end | shuf))
max_iter=2
iter=0

# Loop over the permutation array
for number in "${perm_array[@]}"; do
    if ((iter >= max_iter)); then
        break  # Exit the loop if iter exceeds or equals max_iter
    fi
    data=$(dirname "${BASH_SOURCE[0]}")/datagen/_datagen$number.csv
    data=$(readlink -f "$data")
    log_command bendsql --dsn \"$DSN\" -q \"INSERT INTO TABLE example VALUES\" -d @$data
    ((iter++))  # Increment the iter counter
done