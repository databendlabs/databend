// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, ReactNode } from 'react';
interface IProps {
  children?: ReactNode;
}
const DetailsWrap: FC<IProps> = ({children}): ReactElement=> {
  return (
    <div className="details-wrap">
      {children}
    </div>
  );
};
export default DetailsWrap;