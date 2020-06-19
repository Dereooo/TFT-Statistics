'''

Part of a round-based statistical analysis of our TFT matches collection.

Here, the goal is to analyze each champion (unit) performance and create a metric to rank them.

Assumes you have a MongoDB collection of matches, saved in the same format as fetched from Riot API.

There is a sample_match_info.json on this folder if you're not familiar with the format.

I've implemented it MapReduce-style for 2 main reasons:

1) Although doing everything inside the aggregation pipeline is usually faster, it's less scalable, since you can run into memory usage issues; and

2) It's easier to visualize what is being done due the coded-as-spoken nature of the python language. If needed, tweaks could be made to improve performance, however my priority here was clarity.

'''

from datetime import datetime
from pymongo import MongoClient
import json

if __name__ == "__main__":
    
    # Collection of matches
    client = MongoClient()
    db = client['tfstacticsDB']
    matches_collection = db.matches

    # Pipeline for the query
    # This would be the map stage
    pipeline = [

        # Filter ranked matches
        { "$match": { "info.queue_id" : 1100 } },

        # Optional limit for testing
        # { '$limit': 1000 },

        # Unwind participants
        { "$unwind" : "$info.participants" },

        # Map the units field to their size (number of units)
        { '$addFields' : { 'num_units' : { '$size' : '$info.participants.units' } } },

        # Optional filtering by number of units
        # My goal here is to exclude obviously afk players
        { '$match' : { 'num_units' : { '$gte' : 6 } } },

        # Optional filtering by last round
        # The last round field represents the round in which the player died/won the game. However, there are some rounds in which you don't fight (carousel round) or fight an npc (PVE round). If a player dies during those turns it could mean he forfeited, rage quit, sold all his units, or things like that. There's a very low chance of it happening (less than 1 percent of our sample), but I thought best to exclude those rounds from the analysis altogether. 

        { '$match' : { 'info.participants.last_round' : { '$nin' : [ 11, 15, 18, 22, 25, 29, 32, 36, 39, 43, 46, 50 ] } } },

        # Map the units field to unit id, since we won't be using tier/items at this point
        { '$addFields' : {
            'units': {
                '$map' : { 
                    "input": "$info.participants.units",
                    "as": "unit",
                    "in": "$$unit.character_id",
                }
            },
        }},

        # Project the needed fields
        { '$project' : {
            '_id': 0,
            'last_round': '$info.participants.last_round',
            'placement': '$info.participants.placement',
            'num_units': 1,
            'units': 1,
         } },
    ]

    # Timing query
    start = datetime.now()

    print('Starting analysis...')

    # Executes aggregation
    # If you decide to use $grouping inside the pipeline you might need to set allowDiskUse to True
    query = db.matches.aggregate(pipeline, allowDiskUse=False)

    '''
    Now we compile the results into a dictionary
    This would be the reduce stage

    The end format of the results dictionary is:

    results = {
        round1: {
            champion1: {
                count: X,
                avg_place: Y,
            },
            champion2: {
                count: X,
                avg_place: Y,
            },
            ...,
            total: {
                count: X,
                avg_place: Y,
            }
        },
        round2: {
            champion1: {
                count: X,
                avg_place: Y,
            },
            champion2: {
                count: X,
                avg_place: Y,
            },
            ...,
            total: {
                count: X,
                avg_place: Y,
            }
        },
        ...,
        total: {
            champion1: {
                count: X,
                avg_place: Y,
            },
            champion2: {
                count: X,
                avg_place: Y,
            },
            ...,
            total: {
                count: X,
                avg_place: Y,
            }
        }
    }
    '''
    results = {
        'total': {
            'total': {
                'count': 0,
                'avg_place': 0,
            }
        },
    }

    for player in query:

        # Number of units the player has
        num_units = player['num_units']

        # I'll also exclude players with duplicate units, which you may decide not to do
        # The main reason is tied to my choice of features right below
        # In any case, if you decide to include them, they should be dealt with in some way
        if len(set(player['units'])) != num_units:
            continue
        
        # Player placement, used to calculate the avg_place
        place = player['placement']

        # Total count and placement
        results['total']['total']['count'] += 1
        results['total']['total']['avg_place'] += place

        # Player last round, the main component of the analysis
        last_round = player['last_round']

        if not last_round in results:
            results[last_round] = {
                'total': {
                    'count': 1,
                    'avg_place': place,
                }
            }
        else:
            results[last_round]['total']['count'] += 1
            results[last_round]['total']['avg_place'] += place

        '''
        Now we calculate the count, avg place and damage for individual features and their combinations

        Notes: We first calculate the cum_sum and only in the end we divide it by the count to get the actual average, as reduces usually do
        
        Since python dict keys need to be hashable objects we transform each feature into a string-key
        '''

        # This is the list of string-keys to be added to the results dictionary
        # We gather all the keys first and after that we add them to results, that way we don't repeat code
        keys_list = []

        # feature: Unit was used by the player
        # key: unitName
        unitName_list = map(lambda x: x.split('_')[-1], player['units'])
        keys = map(lambda x: f'{x}', unitName_list)
        keys_list += list(keys)

        # Now we add the keys to the final results dictionary
        for key in keys_list:
            if not key in results[last_round]:
                results[last_round][key] = {
                        'count': 1,
                        'avg_place': place,
                }
            else:
                results[last_round][key]['count'] += 1
                results[last_round][key]['avg_place'] += place
            
            if not key in results['total']:
                results['total'][key] = {
                        'count': 1,
                        'avg_place': place,
                }
            else:
                results['total'][key]['count'] += 1
                results['total'][key]['avg_place'] += place
    
    # Transform the cum into avg for all keys
    for key, dictionary in results.items():
        for nested_key, nested_dictionary in dictionary.items():
            nested_dictionary['avg_place'] /= nested_dictionary['count']

    end = datetime.now()

    print(f'It took {end-start} for the analysis to finish.')

    filename = f'./results/champion_round_base_analysis_{datetime.now():%d-%m-%Y-%H-%M-%S}.json'

    with open(filename, 'w') as f:
        json.dump(results, f)

    print(f'Saved as {filename}.')


