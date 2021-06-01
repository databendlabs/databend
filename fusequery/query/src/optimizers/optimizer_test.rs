use common_planners::*;

pub fn generate_partitions(workers: u64, total: u64) -> Partitions {
    let part_size = total / workers;
    let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(Partition {
            name: format!("{}-{}-{}", total, 0, total,),
            version: 0,
        })
    } else {
        for part in 0..workers {
            let part_begin = part * part_size;
            let mut part_end = (part + 1) * part_size;
            if part == (workers - 1) && part_remain > 0 {
                part_end += part_remain;
            }
            partitions.push(Partition {
                name: format!("{}-{}-{}", total, part_begin, part_end,),
                version: 0,
            })
        }
    }
    partitions
}
