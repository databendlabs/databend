CREATE TABLE variant_test
(
    Id  Int NOT NULL,
    Var Variant NOT NULL
);

CREATE TABLE variant_test2
(
    a Boolean NOT NULL,
    b Int NOT NULL,
    c Float NOT NULL,
    d String NOT NULL,
    e Date NOT NULL,
    f Timestamp NOT NULL,
    g Array(Int) NOT NULL,
    h Tuple(Int, String) NOT NULL,
    i Variant NOT NULL
);
