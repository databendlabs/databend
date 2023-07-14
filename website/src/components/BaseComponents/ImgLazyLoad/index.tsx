// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, useState } from 'react';
// refs: https://databendcloud.github.io/databend-logos/
import { LightDatabendSingleSvg } from 'databend-logos';
import { LazyLoadImage } from 'react-lazy-load-image-component';
import clsx from 'clsx';
interface IProps {
  src: string;
  width?: number | string;
  className?: string;
}
const LoadLazyImg: FC<IProps> = ({src, width, className}): ReactElement=> {
  const [isLoaded, setIsLoaded] = useState(false);
  const handleImageLoad = () => {
    setIsLoaded(true);
  };
  return (
    <>
    {!isLoaded && <LightDatabendSingleSvg width={width}></LightDatabendSingleSvg>}
    <LazyLoadImage
      onLoad={()=> handleImageLoad()}
      className={clsx('g-w100', className, isLoaded ? 'g-db' :'g-dn')}
      src={src} />
    </>
    
  );
};
LoadLazyImg.defaultProps = {
  width: "100%"
}
export default LoadLazyImg;