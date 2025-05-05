// The purpose of this module is to determine the winner for each trophy

// importing
use polars::prelude::*;
use polars::prelude::sort::options::SortMultipleOptions;
use polars::datatypes::AnyValue;

use crate::metrics::team_final_metric_df;
use crate::metrics::defensemen_df;
use crate::metrics::forward_df;

#[allow(warnings)]

// hart memorial trophy --> highest in both off/def metrics
pub fn roy(path: &str, team: &str) -> PolarsResult<String> {
    // This finds the winner of the Roy, aka MVP and the player with the highest cumulative metric
    let mut df = team_final_metric_df(path, team);

    // this takes the lazyframe and sorts it based on total_metric in descending order
    let df = df.clone().lazy()
        .sort(
            ["total_metric"], 
            SortMultipleOptions::default()
                .with_order_descending(true))
        .collect()?; // this converts the lazyframe to a dataframe, which we need for the .get()

    // this takes the sorted dataframe, selects only the column of players names, and gets the first row (the winner)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed in order to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}

// art ross trophy --> highest points
pub fn cullen(path: &str, team: &str) -> PolarsResult<String> {
    // This finds the winner of the Cullen, aka the points leader
    let mut df = team_final_metric_df(path,team);

    // this takes the lazyframe and sorts it based on points in descending order
    let df = df.clone().lazy()
        .sort(
            ["PTS"], 
            SortMultipleOptions::default()
                .with_order_descending(true))
        .collect()?; // this converst the lazyframe to a dataframe, which is needed for the .get()

    // this takes the sorted dataframe, selects only teh column of the players names, and gets the first row (the winner)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed in order to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}

// richard --> highest goals 
pub fn drury(path: &str, team: &str) -> PolarsResult<String> {
    // This finds the Drury winner, aka the goals leader
    let mut df = team_final_metric_df(path, team);

    // this takes the lazyframe and sorts it based on goals in descending order
    let df = df.clone().lazy()
        .sort(
            ["G"], 
            SortMultipleOptions::default()
                .with_order_descending(true))
        .collect()?; // this converts the lazyframe to a dataframe, which is needed for the .get()

    // this takes the sorted dataframe, selects only the players names, and gets the first row (the winner)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed in order to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}

// selke trophy --> forward with the highest def metric
pub fn poulin(path: &str, team: &str) -> PolarsResult<String>{
    // This finds winner of the Poulin, aka the forward with the highest defensive metric
    let mut df = forward_df(path, team);

    // this takes the lazyframe and sorts it based on the defensive metric in descending order
    let df = df.clone().lazy()
        .sort(
            ["defensive_metric"], 
            SortMultipleOptions::default()
                .with_order_descending(true))
        .collect()?; // this converts the lazyframe to a dataframe, which is needed for the .get()

    // this takes the sorted dataframe, selects only the players names, and teh first row (the winner)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}

// norris trophy --> def with the highest def metric
pub fn hutson(path: &str, team: &str) -> PolarsResult<String> {
    // This finds the winner of the Huston, aka the defensemen with the highest defensive metric
    let mut df = defensemen_df(path, team);

    // this takes the lazyframe and sorts it based on the defensive metric in descending order
    let df = df.clone().lazy()
        .sort(
            ["defensive_metric"], 
            SortMultipleOptions::default()
                .with_order_descending(true))
        .collect()?; // this converst hte lazyframe to a dataaframe, which is needed for the .get()

    // this takes teh sorted dataframe and selects only the plaeyers names and the first row (the winner)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}
    
// lady byng trophy --> lowerst pen min/game
pub fn yegorov(path: &str, team: &str) -> PolarsResult<String> {
    // This finds the winner of the Yegorov, aka the player with the lowest penalty mins/game
    let mut df = team_final_metric_df(path, team);

    // this takes the lazyframe and sorts it based on penalty minutes in ascending order
    let df = df.clone().lazy()
        .sort(
            ["penmin_game"],
            SortMultipleOptions::default()
                .with_order_descending(false))
        .collect()?; // this converts the lazyframe to a dataframe, which is needed for the .get()

    // this takes the sorted dataframe and selects only the players names and the first row (the winners)
    let name_col = df
        .column("PLAYER");
    let player_name = name_col?.get(0);

    // this is needed to get the return to work
    match player_name {
        Ok(AnyValue::String(player)) => Ok(player.to_string()),
        _ => Err(PolarsError::ComputeError("Name not string".into()))
    }
}

#[cfg(test)]
mod tests {
    use crate::trophies::roy;
    use crate::trophies::drury;

    #[test]
    // I want to test one "subjective" award (Roy) and one "objective" award (Drury)

    fn roy_test() {
        // Goal: Test that the sorting for the final_metric is working
        let function_answer = roy("src/test-csv.csv", "Team A");
        let function_answer_str = function_answer.unwrap();
        let desired_answer = "Roy Winner";
        assert_eq!(function_answer_str, desired_answer)
    }

    #[test]
    fn drury_test() {
        // Goal: Test that the sorting for goals is working
        let function_answer = drury("src/test-csv.csv", "Team A");
        let function_answer_str = function_answer.unwrap();
        let desired_answer = "Drury Winner";
        assert_eq!(function_answer_str, desired_answer)
    }
}