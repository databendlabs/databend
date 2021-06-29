// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::ErrorCode;
use common_flights::UserInfo;
use sha2::Digest;
use sha2::Sha256;

use crate::user::UserMgr;

#[test]
fn test_add_user() -> common_exception::Result<()> {
    let usr_mgr = UserMgr::new();
    let name = "test_user";
    let pass = "test_pass";
    let salt = "test_salt";
    let pass_hash: [u8; 32] = Sha256::digest(pass.as_bytes()).into();
    let salt_hash: [u8; 32] = Sha256::digest(salt.as_bytes()).into();

    // new user
    usr_mgr.add_user(name, pass, salt)?;

    // duplicated username
    let r = usr_mgr.add_user(name, pass, salt);

    assert!(r.is_err());
    assert_eq!(
        r.unwrap_err().code(),
        ErrorCode::UserAlreadyExists("").code()
    );

    // get user
    let user = usr_mgr.get_user(name)?;
    assert!(user.user_info.is_some());
    let u = user.user_info.unwrap();
    assert_eq!(u.name, name);
    assert_eq!(u.password_sha256, pass_hash,);
    assert_eq!(u.salt_sha256, salt_hash,);

    // get users
    let users = usr_mgr.get_users(&vec![name])?;
    assert_eq!(users.users_info.len(), 1);
    let u = users.users_info[0].as_ref();
    assert!(u.is_some());
    let u = u.unwrap();
    assert_eq!(u.name, name);
    assert_eq!(u.password_sha256, pass_hash,);
    assert_eq!(u.salt_sha256, salt_hash,);

    // drop user
    usr_mgr.drop_user(name)?;
    let user = usr_mgr.get_users(&vec![name])?;
    assert_eq!(user.users_info.len(), 1);
    let u = user.users_info[0].as_ref();
    assert!(u.is_none());

    // get all users
    let mut names = vec![];
    let mut infos = vec![];
    for i in 0..10 {
        let name = format!("u_{}", i);
        let pass = format!("p_{}", i);
        let salt = format!("s_{}", i);
        usr_mgr.add_user(name.clone(), pass.clone(), salt.clone())?;
        names.push(name.clone());
        infos.push(UserInfo {
            name,
            password_sha256: Sha256::digest(pass.as_bytes()).into(),
            salt_sha256: Sha256::digest(salt.as_bytes()).into(),
        })
    }
    let mut users = usr_mgr.get_all_users()?;
    assert_eq!(users.users_info.len(), names.len());
    assert_eq!(users.users_info.sort(), infos.sort());

    // get users
    let mut names = vec![];
    let mut infos = vec![];
    for i in 0..10 {
        let idx = (i % 2) * 100 + i;
        let name = format!("u_{}", idx);
        let pass = format!("p_{}", idx);
        let salt = format!("s_{}", idx);
        names.push(name.clone());
        if i % 2 == 0 {
            infos.push(Some(UserInfo {
                name,
                password_sha256: Sha256::digest(pass.as_bytes()).into(),
                salt_sha256: Sha256::digest(salt.as_bytes()).into(),
            }))
        } else {
            infos.push(None)
        }
    }

    let users = usr_mgr.get_users(&names)?;
    assert_eq!(users.users_info.len(), names.len());
    // order of result should match the order of input
    assert_eq!(users.users_info, infos);

    Ok(())
}
