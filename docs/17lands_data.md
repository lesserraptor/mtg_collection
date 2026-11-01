mission:
take the raw 17lands data and convert into a format we can use instead of the 17lands API. we should have all the same metrics as the 17lands API, but read it from our local DB instead of from the API.

write a script that can process the raw 17lands data into the correct format to be loaded into our local DB. this script should be easy to re-run to process each subsquent sets' data as it releases on 17lands.

note here: we want to have the metrics defined for the "whole" dataset, but we also want to have it defined for just the "top players", too -- that will be the diamond and mythic players only.

location of raw data:
data/game_data_public.FRA.PremierDraft.csv
data/game_data_public.FRA.PremierDraft.csv
data/draft_data_public.FRA.PickTwoDraft.csv
data/game_data_public.FRA.PickTwoDraft.csv

desired data format:
need to be able to quickly look up cards to overlay on the draft. need to be able to see metrics by "all" players and by "only top players". need to be able to look at the metrics by "draft type", too. 

changes to the draft app:
want to add a flag for "late pick up"; ie, a card that shows up AFTER it's usual ALSA position (ie, if also is 3.6, and i see the card on pick 5, that's a "late pick up")

want to be able to toggle between the sets and metrics like do now, but also need a toggle for "all players" vs "top players". also, worth a flag on a card for "top preferred" when the top players value a card higher than the average player.

here's some example cards from the FRA set to help ground the calculations:
card name, draft type, user group, timeframe, GIHWR, OHWR, ALSA, IIH
Ajani Unrelenting, PremierDraft, all users, all time, 73.2% (n=12k), 72.5% (n=4414), 1.2 (n=4201) 20.7pp
Ajani Unrelenting, PremierDraft, top users, all time, 78.6% (n=1270), - (n=431), 1.2 (n=377) 22.8pp
Multiply by Zero, PremierDraft, top users, all time, 64.0% (n=6456), 61.9% (n=2264), 2.6 (n=4383) 6.3pp
Multiply by Zero, PickTwoDraft, all users, all time, 58.9% (n=11k), 56.9% (n=4028), 1.9 (n=11k) 5.0pp




here's the metrics definitions from 17lands:
User Performance Group Definitions

Many people have surmised that showing statistics aggregated across all 17Lands users doesn't quite tell the whole story in some cases. For example, if inexperienced players play a card a lot that good players only utilize in a small number of "correct" situations, the inexperienced players' habits will drag down a card's performance unnecessarily. To help combat this, for some high-volume formats, we've broken out a few performance groups. The exact criteria for these may change over time, but this general premise will still hold:

    Top: Users who have had high win rates (relative to other 17Lands users) in at least 2 of the last 3 sets.
    Middle: Users who have had middle-of-the-road win rates in at least 2 of the last 3 sets.
    Bottom: Users who have had lower win rates in at least 2 of the last 3 sets.

A few things to note here:

    These definitions look at a users' performance across multiple sets in an effort to filter out people who do well or poorly in just one set.
    For ranked formats, we do some mapping of win rates across ranks so that the threshold for performance in Gold, for example, is much higher than the threshold for performance in Mythic.
    We have also separated these by format, as someone who is great at sealed might not be as good at best-of-3 draft, for example.
    Most users don't fall into any of the groups. This is because they haven't been using 17Lands long enough, or because their performance varies too much from set to set. This means that the combined sample sizes for top / middle / bottom data will be much less than the overall ungrouped sample sizes, as the overall data includes games from all users.

Additionally, please note that this is not meant to praise or shame any individual user; we have no intention of revealing which group a user falls into, neither directly nor to the public.
Card Performance Metrics Definitions

As our understanding of the Arena logs has evolved, and as the ways people have tried to use our statistics has grown, we have gone through iterations of how our various card-level statistics are calculated.

In an effort to provide more transparency, we attempt to document clear definitions here. We also document their changes over time, as we expect them to continue to evolve as we add or replace metrics, improve our compute capabilities, and respond to feedback from the community.
Current definitions
In-Draft Statistics

These statistics apply to the draft portion of an event. While in theory we could deduce some information about packs from multiple 17Lands users in the same draft pod, we treat each person’s draft as entirely independent of any other information we have about the same draft pod.
Number Seen

Description: The number of packs in which a card was seen. Cards that come back around when we see them again "on the wheel" are only counted once. When we have information about what was picked but not the other cards that are available (as has been the case for P1P1 for most of the duration of human drafts and also happens occasionally due to connection errors), we do count that one picked card as being "seen".

Caveats: At this time, Arena does not have any duplicates of cards in a pack (unlike paper or MTGO packs that can have duplicates due to foils), but if that were to change, two of a card in a single pack would count twice; we do not do any deduplication.
Average Last Seen At (ALSA)

Description: The average pick number where this card was last seen in packs. When a card comes back around on the wheel, only the second time around counts toward the average.

Caveats: When P1P1 contents are missing, ALSA is slightly elevated for cards that often do not wheel because cards are missing the data from pick 1. Cards that are picked later are less affected because their missing pick 1 data is no longer relevant when pick 9 comes around.
Number Picked

Description: The number of instances of this card picked by 17Lands drafters.
Average Taken At (ATA)

Description: The average pick number at which this card was taken by 17Lands drafters.
In-Game Statistics

These statistics apply to the gameplay portion of an event. They provide data at the granularity of a game (not a match, even for Bo3) because players can and do change their decks between games in the same match or event. If for some reason we find that any of the data for a game is not consistent (e.g. had a card in hand that wasn’t in the deck / sideboard, no deck was linked to the game, etc.), that game doesn’t contribute to any of the in-game statistics.
Number of Games Played (#GP)

Description: The number of games played with this card in the maindeck, multiplied by the number of copies. Playing ten copies of a card in a deck has ten times the weight as playing one copy.
Play Rate (%GP)

Description: The rate at which this card was included in the deck (when available in the card pool). This is weighted by number of games and copies of the card. For example, a deck that played eight games with three maindeck copies of a card and another deck playing four games with one copy of the card in the maindeck and one in the sideboard would result in an 87.5% MD.

Caveats: This also includes games 2 and 3 for best-of-3 matches. Accordingly, you may see bigger differences in play rate for highly conditional cards like Naturalize between best-of-1 and best-of-3 formats as they are brought in from or put into the sideboard.
Games Played Win Rate (GP WR)

Description: The win rate of decks with at least one copy of this card in the maindeck, weighted by the number of copies in the deck. Playing ten copies of a card in a deck has ten times the weight as playing one copy.
Number of Games in Opening Hand (#OH)

Description: The number of games where an instance of this card was in the opening hand. Each instance of a card is counted, so a game having two copies of a card has twice as much weight as an opening hand with only one copy.

Caveats: Note that an "opening hand" is the full hand that was kept after any mulligan decisions. All seven cards that a user decides to keep are counted towards opening hand statistics. Note that this also includes any cards that are ultimately put on the bottom of the library when taking one or more mulligans. This also counts "Leylines" and any other card that might be put on the battlefield or some other zone at the start of the game as well. This metric is biased by the fact that some cards—usually expensive or otherwise hard-to-cast ones—are more likely to contribute to a hand being mulliganed.
Opening Hand Win Rate (OH WR)

Description: The win rate of games where an instance of this card was in the opening hand. Each instance of a card is counted, so a game having two copies of a card has twice as much weight as an opening hand with only one copy.

Caveats: See the note above about the definition of an "opening hand".
Number of Games Drawn (#GD)

Description: The number of times this card was drawn from the deck into hand, not counting the opening hand. This includes cards that were drawn at the start of a turn or due to card drawing abilities. It does not include cards returned to hand from the battlefield or graveyard, tutored, or played from exile. Each copy is counted, so a game having two copies of a card drawn has twice as much weight as a game with only one copy drawn.

Note that "tutoring" is defined as any effect where the player chooses from cards in their library. This includes effects like those of Evolving Wilds and Shrine Steward, but it also includes effects like choosing one of the top N cards of your library with Impulse or Organ Hoarder.

Caveats: It is possible for the same instance of a card to be counted if it is put back into the library and drawn again, so cards that put themselves back in (e.g. Approach of the Second Sun) or are more likely to be targeted by effects like Totally Lost have some bias here.
Games Drawn Win Rate (GD WR)

Description: The win rate of games where an instance of this card was drawn from the deck into hand, not counting cards from the opening hand. Each instance of a card is counted, so a game drawing two copies of a card has twice as much weight as a game drawing only one copy.

Caveats: See the caveats above about the same instance of a card being drawn multiple times.
Number of Games In Hand (#GIH)

Description: The number of times this card was drawn into hand, either in the opening hand or later. Each instance of a card is counted, so a game having a copy of a card in the opening hand and then two more copies drawn later has three times as much weight as an opening hand with only one copy.

Caveats: See the caveats above about the same instance of a card being drawn multiple times. Note that there is a slight possibility that a card is drawn into hand more times than it is in the deck (e.g. if it is shuffled back in).
Games in Hand Win Rate (GIH WR)

Description: The win rate of games where an instance of this card was drawn into hand, either in the opening hand or later. Each instance of a card is counted, so a game having a copy of a card in the opening hand and then two more copies drawn later has three times as much weight as an opening hand with only one copy.

Caveats: See the caveats above about the same instance of a card being drawn multiple times.
Number of Games Not Seen (#GNS)

Description: The copies of a card that were in the maindeck, minus the number of copies that were drawn or tutored ("seen"). If more copies are seen in a game than are in the maindeck, this value is set to 0.
Games Not Seen Win Rate (GNS WR)

Description: The win rate in games where one or more copies of the card was in the maindeck but not seen at any stage of the game, weighted by the number of copies seen.

Caveats: See the notes above for Number of Games Not Seen.
Improvement In Hand (IIH)

Description: The difference between Games in Hand Win Rate and Games Not Seen Win Rate.

Caveats: This metric is a simple difference and does not weight by the number of games in each situation, which may overvalue powerful late-game cards.
Arrows in ALSA, GIH WR, and IIH columns

The arrows indicate that the card is an outlier for the metric. We define an outlier as any value that is at least 1.5 standard deviations away from the mean. You can hover over the value to learn more.
Popups on ALSA, ATA, GP WR, OH WR, GD WR, GIH WR, GNS WR, and IIH columns

When you hover over values in these columns, you can learn:

    How it compares to other cards in the table (e.g., 22 of 230)
    Its z-score (how many standard deviations its value is from the mean)
    The mean and standard deviation of the column

Other Notes
Maindeck and Sideboard

When we use the terms "maindeck" or "sideboard" throughout the site, we are referring to the placement of cards at the start of each game, not match.
Changelog

Unless otherwise indicated, when we change the computation of a statistic, it only affects data for that date moving forward.

2025-08-01: We renamed "Improvement When Drawn" to "Improvement In Hand" to better reflect the definition of the metric. The definition remains the same.

2024-11-12: We changed the computation (and definition) of "opening hand" metrics to include any cards that were in the seven-card hand that was kept after mulligan decisions. The prior definition only included cards that were in hand at the start of turn one, which excluded cards that were put on the bottom of the library after mulligans or those that start the game on the battlefield.

2023-09-21: We added a definition for Play Rate and clarifying note about "maindeck" and "sideboard".

2022-10-16: We updated the calculation for games-drawn and games-in-hand. Prior to this change, tutored cards were unintentionally included in these calculations. Additionally, the games-not-drawn metric was renamed to games-not-seen to account for the fact that it also excludes cards tutored to hand. These changes were backfilled to 2022-09-01, when DMU was released on Arena.

2022-02-27: A major overhaul to in-game statistics outlined here. These changes were backfilled to 2022-02-10, when NEO was released on Arena. Prior to this date GD and GIH included cards bounced to hand, not just drawn from the deck. Additionally, prior to this, GP and GND only counted at most once per game, not per copy of the card.

2021-04-16: Prior to this date, we would only throw out some of the data—Opening Hand, Games Drawn, Games in Hand, Games Not Drawn—if there was a discrepancy between gameplay and deck data, but we would still count the Games Played data. Because of this, we likely included some wins and losses in Games Played data with decks that did not completely match the actual deck played.

2021-03-29: Until this point, we only counted at most one instance of a card for any given game for the Games in Hand data.

2021-03-29: Until this date, we occasionally counted gameplay statistics for multiple deck colors if a user played decks with different color designations in the same event. This most often happened if someone splashed a card in some games but not others.

2021-01-24: Added the ability to filter on time ranges and on a per-maindeck-color basis.

## Status (implemented)

- Ingest CLI (`python -m src.db.draft_metrics_ingest`) with DuckDB engine and pure-Python fallback
- Raw-sums storage in `draft_card_metrics`; all rates derived at read time
- Top group = rank ∈ {diamond, mythic}; "all" group covers everything
- Badges: LATE (pick > ALSA) and TOP+ (ΔGIHWR ≥ 2.0pp with gih_inst ≥ 500 in both groups)
- 17Lands API fallback kept for cards/fields the local data lacks
