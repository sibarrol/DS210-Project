// The purpose of this module is to have the functions necessary for the different operations dealing with the dataframe

// importing
use polars::prelude::*;
#[allow(warnings)]

pub fn read_csv(path: &str) -> LazyFrame {
    let df = LazyCsvReader::new(path)
        .finish()
        .unwrap();
    return df
}

// given a team, return a dataframe of the players on that team
pub fn team_df(path: &str, team: &str) -> LazyFrame {
    let df = read_csv(path);

    let team_df = df.clone()
        .filter(
            (col("Team").eq(lit(team)))
        )
        .with_column(
            col("PLAYER").cast(DataType::String)
        );
    
    return team_df
}

