# webpage app for browsing/analyzing mtga collection

at a high level, this is a webpage that uses an sqlite db of my collection to enable browsing and analyzing the collection.

i intend to use this locally, not on a public facing server. there is only one user and one collection.

## layout

the app should have a left panel, a top panel and a main window.

### top panel

switch between deck mode, full collection mode, and "potential deck" mode. also have a settings option. there should be an "analysis" mode, too (see details in requirements)

### left panel

depending on which mode we are in, show decks, an import deck function and/or filtering criteria.

### main panel

shows the cards either in the collection or the deck, or what has been filtered down to.

## requirements

- deck import/export should support a text format that is used for importing into magic arena.
- filtering should support all the card characteristics used in arena filtering. here's an example: https://draftsim.com/wp-content/uploads/2023/03/image2.png
- if a deck that's been imported has a card that isn't in the collection, show it "grayed out" -- even if we have some of the cards in the collection, but not the full quantity.
- look and feel: i like how moxfield looks: 
	- https://moxfield.com/users/Ashlizzlle
	- https://moxfield.com/decks/oRax6WWSQ0eSYH3vQzaz9Q
- collection view should have a "number of cards per page" selector: default to 20, then offer 50, 75, 100, 200.
- show pictures of cards like moxfield does. you can get the images from scryfall: https://scryfall.com/docs/api/bulk-data
- have a button in settings to update scryfall data, don't do it automatically.
- have a button in settings to update deck data, don't do it automatically. (this will use the collection updater function we've already built)
- if we need to write any code, it should prefer to be written in python


### analysis mode
- for all "potential decks" in the system, find cards that are not in the collection, and count the number of decks they would show up in. sort by this and show cards that would be useful in the most decks first. this is a way to identify cards to craft that have the most utility.
- for all decks in the "potential decks" list, figure out which ones have the smallest nubmer of cards missing and show those.
