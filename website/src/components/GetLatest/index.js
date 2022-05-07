import React from "react";
import { getLatest } from '@site/src/plugins/releaseVersion';
function GetLatest() {
  return (
    <>{getLatest()}</>
  )
}
export default GetLatest;