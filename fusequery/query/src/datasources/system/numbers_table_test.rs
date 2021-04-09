use common_planners::Partition;
use pretty_assertions::assert_eq;

use crate::datasources::system::NumbersTable;
// TODO test ITabl and ITableFUnction
#[test]
fn test_numbers_table_generate_parts() -> anyhow::Result<()> {
    let t = NumbersTable::create("foo");

    {
        // deal with remainder
        let ps = t.generate_parts(3, 11);

        assert_eq!(3, ps.len());
        assert_eq!(
            Partition {
                name: "11-0-3".into(),
                version: 0,
            },
            ps[0]
        );
        assert_eq!(
            Partition {
                name: "11-3-6".into(),
                version: 0,
            },
            ps[1]
        );
        assert_eq!(
            Partition {
                name: "11-6-11".into(),
                version: 0,
            },
            ps[2]
        );
    }

    {
        // total is zero
        let ps = t.generate_parts(3, 0);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "0-0-0".into(),
                version: 0,
            },
            ps[0]
        );
    }
    {
        // only one part, total < workers
        let ps = t.generate_parts(3, 2);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "2-0-2".into(),
                version: 0,
            },
            ps[0]
        );
    }

    Ok(())
}
