# PREMIER DRAFT
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME'

# QUICK DRAFT
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=QuickDraft' --data-urlencode 'time_period=ALL_TIME'

# PICK2 DRAFT
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PickTwoDraft' --data-urlencode 'time_period=ALL_TIME'

# TOP USERS
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME' --data_urlencode 'user_group=top'

# TIME PERIODS
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PickTwoDraft' --data-urlencode 'time_period=ALL_EXCEPT_FIRST_WEEK'

# COLOR
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME' --data-urlencode 'color=w'

# TWO COLORS
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME' --data-urlencode 'color=w~b'

# MULTICOLOR 
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME' --data-urlencode 'color=multicolor'
# MULTICOLOR 
curl --get 'https://www.17lands.com/api/card_data' --data-urlencode 'expansion=FRA' --data-urlencode 'event_type=PremierDraft' --data-urlencode 'time_period=ALL_TIME' --data-urlencode 'color=colorless'
