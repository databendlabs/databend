use geozero::GeozeroGeometry;

use super::Geometry;
use super::GeometryBuilder;

pub struct Wkb<B: AsRef<[u8]>>(pub B);

impl<B: AsRef<[u8]>> TryFrom<Wkb<B>> for Geometry {
    type Error = anyhow::Error;

    fn try_from(value: Wkb<B>) -> Result<Self, Self::Error> {
        let mut p = GeometryBuilder::processor();
        geozero::wkb::Wkb(value.0).process_geom(&mut p)?;
        Ok(p.build())
    }
}

impl TryInto<Wkb<Vec<u8>>> for &Geometry {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Wkb<Vec<u8>>, Self::Error> {
        use geozero::CoordDimensions;
        use geozero::ToWkb;

        Ok(Wkb(
            TryInto::<geo::Geometry>::try_into(self)?.to_ewkb(CoordDimensions::xy(), None)?
        ))
    }
}
