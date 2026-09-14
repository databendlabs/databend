use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::StringType;
use databend_common_io::HybridBitmap;
use itertools::Itertools;

pub(super) fn bitmap_column() -> Column {
    // construct bitmap column with 4 row:
    // 0..5, 1..6, 2..7, 3..8
    const N: u64 = 4;
    let rbs_iter = (0..N).map(|i| {
        let mut rb = HybridBitmap::new();
        for value in i..(i + 5) {
            rb.insert(value);
        }
        rb
    });

    let rbs = rbs_iter
        .map(|rb| {
            let mut data = Vec::new();
            rb.serialize_into(&mut data).unwrap();
            data
        })
        .collect_vec();

    BitmapType::from_data(rbs)
}

pub(super) fn geometry_columns() -> Vec<(&'static str, BlockEntry)> {
    [
        (
            "point",
            StringType::from_data(vec!["POINT(1 1)", "POINT(2 2)", "POINT(3 3)", "POINT(4 4)"]),
        ),
        (
            "point_null",
            StringType::from_data_with_validity(
                vec!["POINT(1 1)", "", "POINT(3 3)", "POINT(4 4)"],
                vec![true, false, true, true],
            ),
        ),
        (
            "point_all_null",
            StringType::from_data_with_validity(
                vec!["", "", "", ""],
                vec![false, false, false, false],
            ),
        ),
        (
            "line_string",
            StringType::from_data(vec![
                "LINESTRING(0 0, 1 1)",
                "LINESTRING(1 1, 2 2)",
                "LINESTRING(2 2, 3 3)",
                "LINESTRING(3 3, 4 4)",
            ]),
        ),
        (
            "line_string_null",
            StringType::from_data_with_validity(
                vec![
                    "LINESTRING(0 0, 1 1)",
                    "",
                    "LINESTRING(2 2, 3 3)",
                    "LINESTRING(3 3, 4 4)",
                ],
                vec![true, false, true, true],
            ),
        ),
        (
            "polygon",
            StringType::from_data(vec![
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))",
                "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                "POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))",
            ]),
        ),
        (
            "mixed_geom",
            StringType::from_data(vec![
                "POINT(0 0)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                "POINT(4 4)",
            ]),
        ),
        (
            "mixed_geom_null",
            StringType::from_data_with_validity(
                vec![
                    "POINT(0 0)",
                    "",
                    "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                    "POINT(4 4)",
                ],
                vec![true, false, true, true],
            ),
        ),
        (
            "point_4326",
            StringType::from_data(vec![
                "SRID=4326;POINT(116.3 39.9)",
                "SRID=4326;POINT(121.4 31.2)",
                "SRID=4326;POINT(113.2 23.1)",
                "SRID=4326;POINT(114.1 22.5)",
            ]),
        ),
        (
            "line_string_4326",
            StringType::from_data(vec![
                "SRID=4326;LINESTRING(116.3 39.9, 121.4 31.2)",
                "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                "SRID=4326;LINESTRING(113.2 23.1, 114.1 22.5)",
                "SRID=4326;LINESTRING(114.1 22.5, 116.3 39.9)",
            ]),
        ),
        (
            "polygon_4326",
            StringType::from_data(vec![
                "SRID=4326;POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))",
                "SRID=4326;POLYGON((121 31, 122 31, 122 32, 121 32, 121 31))",
                "SRID=4326;POLYGON((113 23, 114 23, 114 24, 113 24, 113 23))",
                "SRID=4326;POLYGON((114 22, 115 22, 115 23, 114 23, 114 22))",
            ]),
        ),
        (
            "mixed_3857",
            StringType::from_data(vec![
                "SRID=3857;POINT(12947889.3 4852834.1)",
                "SRID=3857;LINESTRING(13515330.8 3642091.4, 12600089.2 2632873.5)",
                "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
                "SRID=3857;POINT(12959772.9 2551529.8)",
            ]),
        ),
        (
            "mixed_srid",
            StringType::from_data(vec![
                "SRID=4326;POINT(116.3 39.9)",
                "SRID=3857;POINT(12947889.3 4852834.1)",
                "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
            ]),
        ),
        (
            "mixed_srid_null",
            StringType::from_data_with_validity(
                vec![
                    "SRID=4326;POINT(116.3 39.9)",
                    "",
                    "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                    "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
                ],
                vec![true, false, true, true],
            ),
        ),
    ]
    .into_iter()
    .map(|(name, column)| (name, column.into()))
    .collect()
}

pub(super) fn overlapping_geometry_columns() -> Vec<(&'static str, BlockEntry)> {
    [
        (
            "polygon_overlap",
            StringType::from_data(vec![
                "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "POLYGON((1.5 1.5, 2.5 1.5, 2.5 2.5, 1.5 2.5, 1.5 1.5))",
                "POLYGON((1.8 1.8, 2.2 1.8, 2.2 2.2, 1.8 2.2, 1.8 1.8))",
            ]),
        ),
        (
            "polygon_overlap_null",
            StringType::from_data_with_validity(
                vec![
                    "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
                    "",
                    "POLYGON((1.5 1.5, 2.5 1.5, 2.5 2.5, 1.5 2.5, 1.5 1.5))",
                    "POLYGON((1.8 1.8, 2.2 1.8, 2.2 2.2, 1.8 2.2, 1.8 1.8))",
                ],
                vec![true, false, true, true],
            ),
        ),
        (
            "polygon_overlap_all_null",
            StringType::from_data_with_validity(vec!["", "", "", ""], vec![
                false, false, false, false,
            ]),
        ),
    ]
    .into_iter()
    .map(|(name, column)| (name, column.into()))
    .collect()
}
