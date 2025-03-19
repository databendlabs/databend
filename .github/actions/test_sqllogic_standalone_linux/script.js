module.exports = (core) => {
    switch (process.env.DIRS) {
        case 'tpcds':
            const parquet = [
                '--skip_file tpcds_spill_2.test,tpcds_spill_3.test',
                '--skip_file tpcds_spill_1.test,tpcds_spill_3.test',
                '--skip_file tpcds_spill_1.test,tpcds_spill_2.test',
            ][Date.now() % 3];
            core.setOutput('parquet', parquet)
            return
    }
}
