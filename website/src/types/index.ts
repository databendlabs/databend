import { ReactNode } from "react";

export interface ICommonProps {
  children: ReactNode;
  className?: string;
  style?: React.CSSProperties;
  onClick?: ()=> void;
}