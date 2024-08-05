use geozero::error::GeozeroError;

use super::Element;
use super::Geometry;
use super::GeometryBuilder;
use super::ObjectKind;
use super::Visitor;

pub struct GeoJson<S: AsRef<str>>(pub S);

impl<V: Visitor> Element<V> for geojson::GeoJson {
    fn accept(&self, visitor: &mut V) -> Result<(), GeozeroError> {
        use geojson::Feature;
        use geojson::GeoJson;
        use geojson::Geometry;
        use geojson::Value;

        fn visit_points(
            points: &[Vec<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), GeozeroError> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                let (x, y) = normalize_point(p)?;
                visitor.visit_point(x, y, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            visitor: &mut impl Visitor,
            geom: &Geometry,
            feature: Option<&Feature>,
        ) -> Result<(), GeozeroError> {
            if let Some(feature) = feature {
                visitor.visit_feature(&feature.properties)?;
            }
            match &geom.value {
                Value::Point(point) => {
                    let (x, y) = normalize_point(point)?;
                    visitor.visit_point(x, y, false)?;
                    visitor.finish(ObjectKind::Point)
                }
                Value::MultiPoint(points) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        let (x, y) = normalize_point(point)?;
                        visitor.visit_point(x, y, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.finish(ObjectKind::MultiPoint)
                }
                Value::LineString(line) => {
                    visit_points(line, visitor, false)?;
                    visitor.finish(ObjectKind::LineString)
                }
                Value::MultiLineString(lines) => {
                    visitor.visit_lines_start(lines.len())?;
                    for line in lines.iter() {
                        visit_points(line, visitor, true)?;
                    }
                    visitor.visit_lines_end()?;
                    visitor.finish(ObjectKind::MultiLineString)
                }
                Value::Polygon(polygon) => {
                    visitor.visit_polygon_start(polygon.len())?;
                    for ring in polygon {
                        visit_points(ring, visitor, true)?;
                    }
                    visitor.visit_polygon_end(false)?;
                    visitor.finish(ObjectKind::Polygon)
                }
                Value::MultiPolygon(polygons) => {
                    visitor.visit_polygons_start(polygons.len())?;
                    for polygon in polygons {
                        visitor.visit_polygon_start(polygon.len())?;
                        for ring in polygon {
                            visit_points(ring, visitor, true)?;
                        }
                        visitor.visit_polygon_end(true)?;
                    }
                    visitor.visit_polygons_end()?;
                    visitor.finish(ObjectKind::MultiPolygon)
                }
                Value::GeometryCollection(collection) => {
                    visitor.visit_collection_start(collection.len())?;
                    for geom in collection {
                        accept_geom(visitor, geom, None)?;
                    }
                    visitor.visit_collection_end()?;
                    visitor.finish(ObjectKind::Collection)
                }
            }
        }

        let default_point = Geometry::new(Value::Point(vec![f64::NAN, f64::NAN]));

        match self {
            GeoJson::Geometry(geom) => accept_geom(visitor, geom, None),
            GeoJson::Feature(feature) => accept_geom(
                visitor,
                feature.geometry.as_ref().unwrap_or(&default_point),
                Some(feature),
            ),
            GeoJson::FeatureCollection(collection) => {
                visitor.visit_collection_start(collection.features.len())?;
                for featrue in collection {
                    accept_geom(
                        visitor,
                        featrue.geometry.as_ref().unwrap_or(&default_point),
                        Some(featrue),
                    )?;
                }
                visitor.visit_collection_end()?;
                visitor.finish(ObjectKind::Collection)
            }
        }
    }
}

fn normalize_point(point: &[f64]) -> Result<(f64, f64), GeozeroError> {
    if point.len() != 2 {
        Err(GeozeroError::Geometry(
            "coordinates higher than two dimensions are not supported".to_string(),
        ))
    } else {
        Ok((point[0], point[1]))
    }
}

impl<S: AsRef<str>> TryFrom<GeoJson<S>> for Geometry {
    type Error = GeozeroError;

    fn try_from(str: GeoJson<S>) -> Result<Self, Self::Error> {
        let json_struct: geojson::GeoJson = str.0.as_ref().parse()?;
        let mut builder = GeometryBuilder::new();
        json_struct.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<GeoJson<String>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<GeoJson<String>, Self::Error> {
        let mut out: Vec<u8> = Vec::new();
        let mut p = geozero::geojson::GeoJsonWriter::new(&mut out);
        geozero::FeatureAccess::process(self, &mut p, 0)?;

        match String::from_utf8(out) {
            Ok(str) => Ok(GeoJson(str)),
            Err(_) => Err(geozero::error::GeozeroError::Geometry(
                "Invalid UTF-8 encoding".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_json() {
        run_from_json(r#"{"type": "Point", "coordinates": [-122.35,37.55]}"#);
        run_from_json(r#"{"type": "MultiPoint", "coordinates": [[-122.35,37.55],[0,-90]]}"#);
        run_from_json(r#"{"type": "LineString", "coordinates": [[-124.2,42],[-120.01,41.99]]}"#);
        run_from_json(
            r#"{"type": "LineString", "coordinates": [[-124.2,42],[-120.01,41.99],[-122.5,42.01]]}"#,
        );
        run_from_json(
            r#"{"type": "MultiLineString", "coordinates": [[[-124.2,42],[-120.01,41.99],[-122.5,42.01]],[[10,0],[20,10],[30,0]]]}"#,
        );
        run_from_json(
            r#"{"type": "MultiLineString", "coordinates": [[[-124.2,42],[-120.01,41.99]],[[-124.2,42],[-120.01,41.99],[-122.5,42.01],[-122.5,42.01]],[[-124.2,42],[-120.01,41.99],[-122.5,42.01]],[[10,0],[20,10],[30,0]]]}"#,
        );
        run_from_json(
            r#"{"type": "Polygon", "coordinates": [[[17,17],[17,30],[30,30],[30,17],[17,17]]]}"#,
        );
        run_from_json(
            r#"{"type": "Polygon", "coordinates": [[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.8,0.8],[100.8,0.2],[100.2,0.2],[100.2,0.8],[100.8,0.8]]]}"#,
        );
        run_from_json(
            r#"{"type": "MultiPolygon", "coordinates": [[[[-10,0],[0,10],[10,0],[-10,0]]],[[[-10,40],[10,40],[0,20],[-10,40]]]]}"#,
        );
        run_from_json(
            r#"{"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [99,11]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,10]}]}"#,
        );
        run_from_json(
            r#"{"type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-10,0],[0,10],[10,0],[-10,0]]]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,11]}]}"#,
        );
        run_from_json(
            r#"{"type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-10,0],[0,10],[10,0],[-10,0]]]},{"type": "GeometryCollection", "geometries": [{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,11]}]},{"type": "Point", "coordinates": [50,70]}]}"#,
        );
    }

    #[test]
    fn test_from_feature() {
        run_from_json(
            r#"{"type": "Feature", "properties": {"name": "abc"}, "geometry": {"type": "Point", "coordinates": [173,-40]}}"#,
        );
        run_from_json(
            r#"{"type": "Feature", "properties": {"name": "abc"}, "geometry": {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [99,11]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,10]}]}}"#,
        );
    }

    fn run_from_json(want: &str) {
        let geom: crate::Geometry = GeoJson(want).try_into().unwrap();
        let GeoJson(got) = (&geom).try_into().unwrap();

        assert_eq!(want, got)
    }
}
