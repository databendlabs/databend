use thiserror::Error;

macro_rules! as_item {
    ($i:item) => {
        $i
    };
}

macro_rules! build_error_codes {
    ($($body:tt($code:expr)),*) => {
        as_item! {
            #[derive(Error, Debug)]
            pub enum ErrorCodes {
                $(
                    #[error("Code: {}, displayText = {0}.", $code)]
                    $body(String),
                )*
            }
        }
    };
}

build_error_codes! {
    Ok(0),
    MySQLProtocolError(1),
    UnknownTypeOfQuery(2),

    UnknownException(1000),
    TokioError(1001)
}
