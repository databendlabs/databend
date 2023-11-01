// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, ReactNode } from 'react';
interface IProps {
  children?: ReactNode;
}
const StepsWrap: FC<IProps> = ({children}): ReactElement=> {
  return (
    <div className="steps-wrap">
      {children}
    </div>
  );
};
export default StepsWrap;