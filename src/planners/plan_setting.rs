// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[derive(Clone)]
pub struct VarValue {
    pub variable: String,
    pub value: String,
}

#[derive(Clone)]
pub struct SettingPlan {
    pub vars: Vec<VarValue>,
}
