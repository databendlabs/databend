// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use common_meta_types::UserIdentity;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use sqlparser::ast::Ident;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::parser_err;
use crate::sql::statements::DfAlterUser;
use crate::sql::statements::DfAuthOption;
use crate::sql::statements::DfCreateUser;
use crate::sql::statements::DfDropUser;
use crate::sql::statements::DfGrantObject;
use crate::sql::statements::DfGrantStatement;
use crate::sql::statements::DfRevokeStatement;
use crate::sql::statements::DfShowGrants;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_create_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };

        let auth_option = self.parse_auth_option()?;

        let create = DfCreateUser {
            if_not_exists,
            name,
            hostname,
            auth_options: auth_option,
        };

        Ok(DfStatement::CreateUser(create))
    }

    pub(crate) fn parse_alter_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_current_user = self.consume_token("USER")
            && self.parser.expect_token(&Token::LParen).is_ok()
            && self.parser.expect_token(&Token::RParen).is_ok();
        let name = if !if_current_user {
            self.parser.parse_literal_string()?
        } else {
            String::from("")
        };
        let hostname = if !if_current_user {
            if self.consume_token("@") {
                self.parser.parse_literal_string()?
            } else {
                String::from("%")
            }
        } else {
            String::from("")
        };

        let auth_option = self.parse_auth_option()?;

        let alter = DfAlterUser {
            if_current_user,
            name,
            hostname,
            auth_option,
        };

        Ok(DfStatement::AlterUser(alter))
    }

    pub(crate) fn parse_drop_user(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };
        let drop = DfDropUser {
            if_exists,
            name,
            hostname,
        };
        Ok(DfStatement::DropUser(drop))
    }

    // Grant.
    pub(crate) fn parse_grant(&mut self) -> Result<DfStatement, ParserError> {
        let privileges = self.parse_privileges()?;
        if !self.parser.parse_keyword(Keyword::ON) {
            return self.expected("keyword ON", self.parser.peek_token());
        }
        let on = self.parse_grant_object()?;
        if !self.parser.parse_keyword(Keyword::TO) {
            return self.expected("keyword TO", self.parser.peek_token());
        }
        let (name, hostname) = self.parse_user_identity()?;
        let grant = DfGrantStatement {
            name,
            hostname,
            on,
            priv_types: privileges,
        };
        Ok(DfStatement::GrantPrivilege(grant))
    }

    // Revoke.
    pub(crate) fn parse_revoke(&mut self) -> Result<DfStatement, ParserError> {
        let privileges = self.parse_privileges()?;
        if !self.parser.parse_keyword(Keyword::ON) {
            return self.expected("keyword ON", self.parser.peek_token());
        }
        let on = self.parse_grant_object()?;
        if !self.parser.parse_keyword(Keyword::FROM) {
            return self.expected("keyword FROM", self.parser.peek_token());
        }
        let (username, hostname) = self.parse_user_identity()?;
        let revoke = DfRevokeStatement {
            username,
            hostname,
            on,
            priv_types: privileges,
        };
        Ok(DfStatement::RevokePrivilege(revoke))
    }

    // Show grants.
    pub(crate) fn parse_show_grants(&mut self) -> Result<DfStatement, ParserError> {
        // SHOW GRANTS
        if !self.consume_token("FOR") {
            return Ok(DfStatement::ShowGrants(DfShowGrants {
                user_identity: None,
            }));
        }

        // SHOW GRANTS FOR 'u1'@'%'
        let (username, hostname) = self.parse_user_identity()?;
        Ok(DfStatement::ShowGrants(DfShowGrants {
            user_identity: Some(UserIdentity { username, hostname }),
        }))
    }

    fn parse_user_identity(&mut self) -> Result<(String, String), ParserError> {
        let username = self.parser.parse_literal_string()?;
        let hostname = if self.consume_token("@") {
            self.parser.parse_literal_string()?
        } else {
            String::from("%")
        };
        Ok((username, hostname))
    }

    /// Parse a possibly qualified, possibly quoted identifier or wild card, e.g.
    /// `*` or `myschema`.*. The sub string pattern like "db0%" is not in planned.
    fn parse_grant_object(&mut self) -> Result<DfGrantObject, ParserError> {
        let chunk0 = self.parse_grant_object_pattern_chunk()?;
        // "*" as current db or "table" with current db
        if !self.consume_token(".") {
            if chunk0.value == "*" {
                return Ok(DfGrantObject::Database(None));
            }
            return Ok(DfGrantObject::Table(None, chunk0.value));
        }
        let chunk1 = self.parse_grant_object_pattern_chunk()?;

        // *.* means global
        if chunk1.value == "*" && chunk0.value == "*" {
            return Ok(DfGrantObject::Global);
        }
        // *.table is not allowed
        if chunk0.value == "*" {
            return self.expected("whitespace", Token::Period);
        }
        // db.*
        if chunk1.value == "*" {
            return Ok(DfGrantObject::Database(Some(chunk0.value)));
        }
        // db.table
        Ok(DfGrantObject::Table(Some(chunk0.value), chunk1.value))
    }

    /// Parse a chunk from the object pattern, it might be * or an identifier
    fn parse_grant_object_pattern_chunk(&mut self) -> Result<Ident, ParserError> {
        if self.consume_token("*") {
            return Ok(Ident::new("*"));
        }
        let token = self.parser.peek_token();
        self.parser
            .parse_identifier()
            .or_else(|_| self.expected("identifier or *", token))
    }

    fn parse_auth_option(&mut self) -> Result<DfAuthOption, ParserError> {
        let exist_not = self.parser.parse_keyword(Keyword::NOT);
        let exist_identified = self.consume_token("IDENTIFIED");

        if exist_not {
            if !exist_identified {
                parser_err!("expect IDENTIFIED after NOT")
            } else {
                Ok(DfAuthOption::no_password())
            }
        } else if !exist_identified {
            Ok(DfAuthOption::default())
        } else {
            let arg_with = if self.consume_token("WITH") {
                Some(self.parser.parse_literal_string()?)
            } else {
                None
            };
            if arg_with == Some("no_password".to_string()) {
                Ok(DfAuthOption::no_password())
            } else {
                let auth_string = if self.parser.parse_keyword(Keyword::BY) {
                    Some(self.parser.parse_literal_string()?)
                } else {
                    None
                };
                Ok(DfAuthOption {
                    auth_type: arg_with,
                    by_value: auth_string,
                })
            }
        }
    }

    fn parse_privileges(&mut self) -> Result<UserPrivilegeSet, ParserError> {
        let mut privileges = UserPrivilegeSet::empty();
        loop {
            match self.parser.next_token() {
                Token::Word(w) => match w.keyword {
                    // Keyword::USAGE => privileges.set_privilege(UserPrivilegeType::Usage),
                    Keyword::CREATE => {
                        if self.consume_token("USER") {
                            privileges.set_privilege(UserPrivilegeType::CreateUser)
                        } else if self.consume_token("ROLE") {
                            privileges.set_privilege(UserPrivilegeType::CreateRole)
                        } else {
                            privileges.set_privilege(UserPrivilegeType::Create)
                        }
                    }
                    Keyword::DROP => privileges.set_privilege(UserPrivilegeType::Drop),
                    Keyword::ALTER => privileges.set_privilege(UserPrivilegeType::Alter),
                    Keyword::SELECT => privileges.set_privilege(UserPrivilegeType::Select),
                    Keyword::INSERT => privileges.set_privilege(UserPrivilegeType::Insert),
                    Keyword::UPDATE => privileges.set_privilege(UserPrivilegeType::Update),
                    Keyword::DELETE => privileges.set_privilege(UserPrivilegeType::Delete),
                    // TODO: uncomment this after sqlparser-rs accepts the SUPER keyword
                    // Keyword::SUPER => privileges.set_privilege(UserPrivilegeType::Super)
                    Keyword::GRANT => privileges.set_privilege(UserPrivilegeType::Grant),
                    Keyword::ALL => {
                        privileges.set_all_privileges();
                        // GRANT ALL [PRIVILEGES]
                        self.consume_token("PRIVILEGES");
                        break;
                    }
                    _ => return self.expected("privilege type", Token::Word(w)),
                },
                unexpected => return self.expected("privilege type", unexpected),
            };
            if !self.parser.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(privileges)
    }
}
