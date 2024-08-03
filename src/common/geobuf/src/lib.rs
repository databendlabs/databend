#![feature(iter_map_windows)]

mod builder;
mod geo_adapter;
mod geo_generated;
mod geojson_adapter;
mod wkb_addapter;
mod wkt_adapter;

use builder::GeometryBuilder;
use geo_generated::geo_buf;
pub use geojson_adapter::GeoJson;
use geozero::error::GeozeroError;
use geozero::error::Result as GeoResult;
use ordered_float::OrderedFloat;
pub use wkb_addapter::Wkb;
pub use wkt_adapter::Wkt;

// A columnar Geometry/Geography format is proposed here to provide support for the storage and computation of geospatial features.
//
// This column format has five columns, namely the point column, which consists of the x column, the y column, and the point_offset column,
// and the rest of the information is serialized in a binary column, which consists of the data column and the offset column, see Column
//
// Format compatibility
//
// EWKB supports up to 4 dimensions, here only 2 dimensions are supported, in line with snowflake, and new types can be added to support higher dimensions.
// https://docs.snowflake.com/en/sql-reference/data-types-geospatial#geometry-data-type
//
// See geo.fbs for details on other compatibility designs.
//
// References WKT WKB EWKT EWKB GeoJSON Spec.
// https://libgeos.org/specifications/wkb/#standard-wkb
// https://datatracker.ietf.org/doc/html/rfc7946
//
// Why the columnar Geometry/Geography format?
//
// * For smaller storage volumes, see the test below.
// * The problem of compression coding of floating-point columns has a large number of readily available research results that can be introduced at low cost,
// which is conducive to compression of storage space and improvement of io efficiency.
// * Floating-point columns have min max sparse indexes, very similar to the R tree indexes widely used in geospatial features, and are very cheap to implement and maintain.
// * Facilitates the implementation of filtered push-down to the storage layer, and vectorized computation.
// * On the downside, geospatial functions become significantly more expensive to implement and maintain, requiring a significant amount of work.
//
// Why use flatbuffer?
//
// flatbuffer provides delayed deserialization , and partial deserialization capabilities , to facilitate the writing of high-performance implementation .
// In addition, code generation is easy to adapt to the design.
// However, flatbuffer has its drawbacks. Similar to protobuffer, flatbuffer is an extensible, compatible format, and in order to provide extensibility and compatibility, a vtable is deposited in the data, resulting in an inflated size.
// Once the design is stabilized, consider implementing it in other ways.
//

pub struct Column {
    buf: Vec<u8>,
    buf_offsets: Vec<u64>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u64>,
}

pub struct Geometry {
    buf: Vec<u8>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

pub struct BoundingBox {
    pub xmin: f64,
    pub xmax: f64,
    pub ymin: f64,
    pub ymax: f64,
}

impl Geometry {
    pub fn bounding_box(&self) -> BoundingBox {
        fn cmp(a: &f64, b: &f64) -> std::cmp::Ordering {
            std::cmp::Ord::cmp(&OrderedFloat(*a), &OrderedFloat(*b))
        }
        BoundingBox {
            xmin: self
                .column_x
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            xmax: self
                .column_x
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
            ymin: self
                .column_y
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            ymax: self
                .column_y
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
        }
    }

    pub fn memory_size(&self) -> usize {
        self.buf.len() + self.column_x.len() * 16
    }
}

pub struct GeometryRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
}

pub struct Geography {
    buf: Vec<u8>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
}

pub struct GeographyRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
}

trait Visitor {
    fn visit_point(&mut self, x: f64, y: f64, multi: bool) -> GeoResult<()>;

    fn visit_points_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_points_end(&mut self, multi: bool) -> GeoResult<()>;

    fn visit_lines_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_lines_end(&mut self) -> GeoResult<()>;

    fn visit_polygon_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_polygon_end(&mut self, multi: bool) -> GeoResult<()>;

    fn visit_polygons_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_polygons_end(&mut self) -> GeoResult<()>;

    fn visit_collection_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_collection_end(&mut self) -> GeoResult<()>;

    fn finish(&mut self, kind: geo_buf::ObjectKind) -> GeoResult<()>;
}

trait Element<V: Visitor> {
    fn accept(&self, visitor: &mut V) -> GeoResult<()>;
}

#[cfg(test)]
mod tests {
    use geozero::CoordDimensions;
    use geozero::ToWkb;

    use super::*;

    #[test]
    fn test_from_wkt() {
        run_from_wkt(&"POINT(-122.35 37.55)"); // geo_buf:17, ewkb:21, points:16
        run_from_wkt(&"MULTIPOINT(-122.35 37.55,0 -90)"); // geo_buf:33, ewkb:51, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99)"); // geo_buf:33, ewkb:41, points:32
        run_from_wkt(&"LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)"); // geo_buf:49, ewkb:57, points:48

        // geo_buf:141, ewkb:123, points:96
        run_from_wkt(&"MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
        // geo_buf:245, ewkb:237, points:192
        run_from_wkt(
            &"MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
        );
        // geo_buf:121, ewkb:93, points:80
        run_from_wkt(&"POLYGON((17 17,17 30,30 30,30 17,17 17))");
        // geo_buf:205, ewkb:177, points:160
        run_from_wkt(
            &"POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
        );
        // geo_buf:185, ewkb:163, points:128
        run_from_wkt(&"MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
        // geo_buf:205, ewkb:108, points:80
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
        );
        // geo_buf:281, ewkb:164, points:128
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
        );
        // geo_buf:353, ewkb:194, points:144
        run_from_wkt(
            &"GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
        );
    }

    fn run_from_wkt(want: &str) {
        let geom = Geometry::try_from(Wkt(want)).unwrap();
        let ewkb = geozero::wkt::Wkt(want)
            .to_ewkb(CoordDimensions::xy(), None)
            .unwrap();
        println!(
            "geo_buf:{}, ewkb:{}, points:{}",
            geom.memory_size(),
            ewkb.len(),
            geom.column_x.len() * 16
        );

        let Wkt(got) = (&geom).try_into().unwrap();
        assert_eq!(want, got)
    }
}
