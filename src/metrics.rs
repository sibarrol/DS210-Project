// The purpose of this module is to store all of the calculations needed to make the metrics

//importing
use polars::prelude::*;
use crate::dataframe::team_df;
#[allow(dead_code)]

pub fn team_add_col_df(path: &str, team: &str) -> LazyFrame {
    // This function adds the columns of the components that are needed to calculate the off/def metrics, calculated on a per game basis.
    let mut team_df = team_df(path,team);
    
    // these are the components of the defensive metric
    let team_df = team_df.clone().lazy().with_column(
        (col("BLOCKS") / col("GP")).alias("blocks_game"));

    let team_df = team_df.clone().lazy().with_column(
        (col("PLSMNS") / col("GP")).alias("plsmns_game"));

    // these are the components of the defensive metric
    let team_df = team_df.clone().lazy().with_column(
        (col("G") / col("GP")).alias("goals_game"));
    let team_df = team_df.clone().lazy().with_column(
        (col("PTS") / col("GP")).alias("points_game"));
    let team_df= team_df.clone().lazy().with_column(
        ((col("SHG") + col("PPG")) / col("GP")).alias("special_teams_game"));

    // misc. stat for the lady byng
    let team_df = team_df.clone().lazy().with_column(
        (col("PEN MIN") / col("GP")).alias("penmin_game"));

    return team_df
}

pub fn team_metric_col_df(path: &str, team: &str) -> LazyFrame {
    // This function adds the columns of the offensive and defensive metrics, calculated based on the criteria in the previous function.

    let mut team_df = team_add_col_df(path, team);

    // this adds the final defensive metric
    let team_df = team_df.clone().lazy().with_column(
        (col("blocks_game") + col("plsmns_game")).alias("defensive_metric"));

    // this adds the final offensive metric
    let team_df = team_df.clone().lazy().with_column(
        (col("goals_game") + col("points_game") + col("special_teams_game")).alias("offensive_metric"));

    return team_df
}

pub fn team_final_metric_df(path: &str, team: &str) -> LazyFrame {
    // This function adds the column for the cumulative metric (sum of the offensive and defensive).

    let mut team_df = team_metric_col_df(path, team);

    // this adds the offensive and defensive metrics
    let team_df = team_df.clone().lazy().with_column(
        (col("defensive_metric") + col("offensive_metric")).alias("total_metric"));
        
    return team_df
}

pub fn defensemen_df(path: &str, team: &str) -> LazyFrame {
    // This function creates a LazyFrame that just contains the defensemen from the team.

    let team_df = team_final_metric_df(path, team);

    // this creates the dataframe that only contains the defensemen
    let defenseman_df = team_df.lazy().filter(
        col("Team").eq(lit(team))
        .and(col("Pos").eq(lit("D")))
    );
    return defenseman_df
}

pub fn forward_df(path: &str, team: &str) -> LazyFrame {
    // This function creates a LazyFrame that contains just the forwards from the team.

    let team_df = team_final_metric_df(path,team);

    // this creates the dataframe that only contains the forwards
    let forward_df = team_df.lazy().filter(
        col("Team").eq(lit(team))
        .and(col("Pos").eq(lit("F")))
    );
    return forward_df
}
