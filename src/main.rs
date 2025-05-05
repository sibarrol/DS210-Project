mod trophies;
mod metrics;
mod dataframe;

use std::io;
use std::io::Write;
pub use crate::trophies::roy;
pub use crate::trophies::cullen;
pub use crate::trophies::drury;
pub use crate::trophies::poulin;
pub use crate::trophies::hutson;
pub use crate::trophies::yegorov;

fn main() {
    // input team
    println!("Welcome to the 1st Annual DS210 College Hockey Awards, hosted by Sophia Ibarrola");
    println!("Which team would you like to see the awards for? ");
    io::stdout().flush().unwrap();

    let path = "uscho-pivot-stats-final.csv";
    let mut team = String::new();

    io::stdin()
        .read_line(&mut team)
        .expect("Failed to read team name");

    let team = team.trim();

    // hart memorial trophy --> best overall player; highest in both metrics
    let roy = roy(path, team);
    println!("The winner of the Travis Roy Trophy, awarded to the MVP is: {:?}", roy);

    // art ross trophy --> points
    let cullen = cullen(path,team);
    println!("The winner of the John Cullen Trophy, awarded to the player with the most points is: {:?}", cullen);

    // richard --> goals
    let drury = drury(path,team); 
    println!("The winner of the Chris Drury Trophy, awarded to the player with the most goals is: {:?}", drury);

    // selke trophy --> best defensive forward
    let poulin = poulin(path,team);
    println!("The winner of the Marie-Phliip Poulin Trophy, awarded to the best defensive forward, is: {:?}", poulin);

    // norris trophy --> best defenseman
    let hutson = hutson(path,team);
    println!("The winner of the Cole Hutson Trophy, awarded to the best defenseman, is: {:?}", hutson);

    // lady byng trophy --> sportsmanship
    let yegorov = yegorov(path,team);
    println!("The winner of the Mikhail Yegorov Trophy, awarded to the most sportsmanlike player, is: {:?}", yegorov);
}