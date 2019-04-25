import json
import pandas as pd
import urllib.request as urlreq


API_URL = 'https://api.censusreporter.org/1.0/data/show/{release}?table_ids={table_ids}&geo_ids={geoids}'
TOPIC_KEYS = ['B01001', 'B01001', 'B03002', 'B19001', 'B17001', 'B08006',
    'B11002', 'B12001', 'B13016', 'B25002', 'B25003', 'B25024', 'B25026',
    'B25075', 'B07003', 'B15002', 'B16007', 'B16007', 'B05006', 'B21002']
STATE_FIPS = ['01', '02', '04', '05', '06', '08', '09', '10', '11', '12',
    '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
    '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35',
    '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47',
    '48', '49', '50', '51', '53', '54', '55', '56', '60', '66', '69',
    '72', '78']


# Modified from https://github.com/censusreporter/census-pandas/blob/master/util.py
def get_data(tables=None, geoids=None, release='latest'):
    if geoids is None:
        geoids = ['040|01000US']
    elif isinstance(geoids,str):
        geoids = [geoids]
    if tables is None:
        tables = ['B01001']
    elif isinstance(tables,str):
        tables=[tables]

    url = API_URL.format(table_ids=','.join(tables).upper(),
                         geoids=','.join(geoids),
                         release=release)

    with urlreq.urlopen(url.format(tables, geoids)) as response:
        return json.loads(response.read().decode('utf-8'))


# From https://github.com/censusreporter/census-pandas/blob/master/util.py
def prep_for_pandas(json_data, include_moe=False):
    # Given a dict of dicts as they come from a Census Reporter API call, set it up to be amenable to pandas.DataFrame.from_dict
    result = {}
    for geoid, tables in json_data.items():
        flat = {}
        for table,values in tables.items():
            for kind, columns in values.items():
                if kind == 'estimate':
                    flat.update(columns)
                elif kind == 'error' and include_moe:
                    renamed = dict((k+"_moe",v) for k,v in columns.items())
                    flat.update(renamed)
        result[geoid] = flat
    return result


# Modified from https://github.com/censusreporter/census-pandas/blob/master/util.py
def get_dataframe(tables=None, geoids=None, release='latest',geo_names=False,col_names=False,include_moe=False):
    response = get_data(tables=tables,geoids=geoids,release=release)
    frame = pd.DataFrame.from_dict(prep_for_pandas(response['data'],include_moe),orient='index')
    frame = frame[sorted(frame.columns.values)] # data not returned in order
    if geo_names:
        geo = pd.DataFrame.from_dict(response['geography'],orient='index')
        frame.insert(0,'name',geo['name'])
    if col_names:
        d = {}
        for table_id in response['tables']:
            colname_prepends = []
            columns = response['tables'][table_id]['columns']
            for column_id in columns:
                colname = columns[column_id]['name']
                indent = columns[column_id]['indent']

                # Prepend nested column names
                if indent is not None:
                    if indent > len(colname_prepends) - 1:
                        colname_prepends += [colname]
                    else:
                        colname_prepends = colname_prepends[:indent] + [colname]

                    if indent == 0:
                        d[column_id] = colname
                    else:
                        d[column_id] = " ".join(colname_prepends[1:]) # Never want to prepend "Total:"
                else:
                    d[column_id] = colname

        frame = frame.rename(columns=d)

    # Add geoid column
    parsed_geoids = sorted(response['geography'].keys())[1:] # First one is parent
    frame.insert(1, 'geoid', parsed_geoids)

    return frame


def get_dataframe_simple(topic_num, topic, geo):
    response = get_data(tables=topic, geoids=geo, release='latest')
    data = pd.DataFrame.from_dict(prep_for_pandas(response['data'], False), orient='index')
    data = data[sorted(data.columns.values)] # data not returned in order

    # geo is indexed by geoid
    geo = pd.DataFrame.from_dict(response['geography'], orient='index')
    curated_data = pd.DataFrame.from_dict(geo['name'].iloc[1:])

    # Add geoid column
    parsed_geoids = sorted(response['geography'].keys())[1:] # First one is parent
    curated_data.insert(1, 'geoid', parsed_geoids)

    # Column curation
    if topic_num == 0: # Age
        under_18 = data['B01001003'] + data['B01001004'] + data['B01001005'] + \
            data['B01001006'] + data['B01001027'] + data['B01001028'] + \
            data['B01001029'] + data['B01001030']
        curated_data.insert(2, 'Under 18', under_18)

        eighteen_to_64 = data['B01001007'] + data['B01001008'] + \
            data['B01001009'] + data['B01001010'] + data['B01001011'] + \
            data['B01001012'] + data['B01001013'] + data['B01001014'] + \
            data['B01001015'] + data['B01001016'] + data['B01001017'] + \
            data['B01001018'] + data['B01001019'] + data['B01001031'] + \
            data['B01001032'] + data['B01001033'] + data['B01001034'] + \
            data['B01001035'] + data['B01001036'] + data['B01001037'] + \
            data['B01001038'] + data['B01001039'] + data['B01001040'] + \
            data['B01001041'] + data['B01001042'] + data['B01001043']
        curated_data.insert(3, '18 to 64', eighteen_to_64)

        over_65 = data['B01001020'] + data['B01001021'] + \
            data['B01001022'] + data['B01001023'] + data['B01001024'] + \
            data['B01001025'] + data['B01001044'] + data['B01001045'] + \
            data['B01001046'] + data['B01001047'] + data['B01001048'] + \
            data['B01001049']
        curated_data.insert(4, 'Over 65', over_65)

    elif topic_num == 1: # Sex
        curated_data.insert(2, 'Male', data['B01001002'])
        curated_data.insert(3, 'Female', data['B01001026'])

    elif topic_num == 2: # Race
        curated_data.insert(2, 'White', data['B03002003'])
        curated_data.insert(3, 'Black', data['B03002004'])
        curated_data.insert(4, 'Native', data['B03002005'])
        curated_data.insert(5, 'Asian', data['B03002006'])
        curated_data.insert(5, 'Islander', data['B03002007'])
        curated_data.insert(5, 'Other', data['B03002008'])
        curated_data.insert(5, 'Two or More', data['B03002009'])
        curated_data.insert(6, 'Hispanic', data['B03002012'])

    elif topic_num == 3: # Household Income
        under_50k = data['B19001002'] + data['B19001003'] + \
            data['B19001004'] + data['B19001005'] + data['B19001006'] + \
            data['B19001007'] + data['B19001008'] + data['B19001009'] + \
            data['B19001010']
        curated_data.insert(2, 'Under $50K', under_50k)

        fifty_to_100k = data['B19001011'] + data['B19001012'] + data['B19001013']
        curated_data.insert(3, '$50K to $100K', fifty_to_100k)

        hundred_to_200k = data['B19001014'] + data['B19001015'] + data['B19001016']
        curated_data.insert(4, '$100K to $200K', hundred_to_200k)

        curated_data.insert(5, 'Over $200K', data['B19001017'])

    elif topic_num == 4: # Poverty
        poverty = data['B17001004'] + data['B17001005'] + \
            data['B17001006'] + data['B17001007'] + data['B17001008'] + \
            data['B17001009'] + data['B17001018'] + data['B17001019'] + \
            data['B17001020'] + data['B17001021'] + data['B17001022'] + \
            data['B17001023']
        curated_data.insert(2, 'Poverty, Children (Under 18)', poverty)

        non_poverty = data['B17001033'] + data['B17001034'] + \
            data['B17001035'] + data['B17001036'] + data['B17001037'] + \
            data['B17001038'] + data['B17001047'] + data['B17001048'] + \
            data['B17001049'] + data['B17001050'] + data['B17001051'] + \
            data['B17001052']
        curated_data.insert(3, 'Non-poverty, Children (Under 18)', non_poverty)

        poverty = data['B17001015'] + data['B17001016'] + data['B17001029'] + data['B17001030']
        curated_data.insert(4, 'Poverty, Seniors (65 and Over)', poverty)

        non_poverty = data['B17001044'] + data['B17001045'] + data['B17001058'] + data['B17001059']
        curated_data.insert(5, 'Non-poverty, Seniors (65 and Over)', non_poverty)

    elif topic_num == 5: # Transportation to Work
        curated_data.insert(2, 'Drove Alone', data['B08006003'])
        curated_data.insert(3, 'Carpooled', data['B08006004'])
        curated_data.insert(4, 'Public Transit', data['B08006008'])
        curated_data.insert(5, 'Bicycle', data['B08006014'])
        curated_data.insert(6, 'Walked', data['B08006015'])
        curated_data.insert(7, 'Other', data['B08006016'])
        curated_data.insert(8, 'Worked at Home', data['B08006017'])

    elif topic_num == 6: # Population by Household Type
        curated_data.insert(2, 'Married Couples', data['B11002003'])
        curated_data.insert(3, 'Male Householder', data['B11002006'])
        curated_data.insert(4, 'Female Householder', data['B11002009'])
        curated_data.insert(5, 'Non-family', data['B11002012'])

    elif topic_num == 7: # Marital Status by Sex
        curated_data.insert(2, 'Never Married: Male', data['B12001003'])
        curated_data.insert(3, 'Never Married: Female', data['B12001012'])
        curated_data.insert(4, 'Married: Male', data['B12001004'])
        curated_data.insert(5, 'Married: Female', data['B12001013'])
        curated_data.insert(6, 'Divorced: Male', data['B12001010'])
        curated_data.insert(7, 'Divorced: Female', data['B12001019'])
        curated_data.insert(8, 'Windowed: Male', data['B12001009'])
        curated_data.insert(9, 'Windowed: Female', data['B12001018'])

    elif topic_num == 8: # Women who gave birth during past year, by age group
        curated_data.insert(2, '15 to 19', data['B13016003'])
        curated_data.insert(3, '20 to 24', data['B13016004'])
        curated_data.insert(4, '25 to 29', data['B13016005'])
        curated_data.insert(5, '30 to 34', data['B13016006'])
        curated_data.insert(6, '35 to 39', data['B13016007'])
        curated_data.insert(7, '40 to 44', data['B13016008'])
        curated_data.insert(8, '45 to 50', data['B13016009'])

    elif topic_num == 9: # Occupied vs. Vacant Housing
        curated_data.insert(2, 'Occupied', data['B25002002'])
        curated_data.insert(3, 'Vacant', data['B25002003'])

    elif topic_num == 10: # Ownership of Occupied Units
        curated_data.insert(2, 'Owner Occupied', data['B25003002'])
        curated_data.insert(3, 'Renter Occupied', data['B25003003'])

    elif topic_num == 11: # Types of Structure
        curated_data.insert(2, 'Single Unit', data['B25024002'] + data['B25024003'])
        curated_data.insert(3, 'Multi-unit', data['B25024004'] + \
            data['B25024005'] + data['B25024006'] + data['B25024007'] + \
            data['B25024008'] + data['B25024009'])
        curated_data.insert(4, 'Mobile Home', data['B25024010'])
        curated_data.insert(5, 'Vehicle', data['B25024011'])

    elif topic_num == 12: # Year moved in, by population
        curated_data.insert(2, 'Before 1970', data['B25026008'] + data['B25026015'])
        curated_data.insert(3, '1970s', data['B25026007'] + data['B25026014'])
        curated_data.insert(4, '1980s', data['B25026006'] + data['B25026013'])
        curated_data.insert(5, '1990s', data['B25026005'] + data['B25026012'])
        curated_data.insert(6, '2000 to 2004', data['B25026004'] + data['B25026011'])
        curated_data.insert(7, 'Since 2005', data['B25026003'] + data['B25026010'])

    elif topic_num == 13: # Value of owner-occupied housing units
        curated_data.insert(2, 'Under $100K', data['B25075002'] + \
            data['B25075003'] + data['B25075004'] + data['B25075005'] + \
            data['B25075006'] + data['B25075007'] + data['B25075008'] + \
            data['B25075009'] + data['B25075010'] + data['B25075011'] + \
            data['B25075012'] + data['B25075013'] + data['B25075014'])
        curated_data.insert(3, '$100K to $200K', data['B25075015'] + \
            data['B25075016'] + data['B25075017'] + data['B25075018'])
        curated_data.insert(4, '$200K to $300K', data['B25075019'] + data['B25075020'])
        curated_data.insert(5, '$300K to $400K', data['B25075021'])
        curated_data.insert(6, '$400K to $500K', data['B25075022'])
        curated_data.insert(7, '$500K to $1M', data['B25075023'] + data['B25075024'])
        curated_data.insert(8, 'Over $1M', data['B25075025'])

    elif topic_num == 14: # Population migration since previous year
        curated_data.insert(2, 'Same House Year Ago', data['B07003004'])
        curated_data.insert(3, 'From Same County', data['B07003007'])
        curated_data.insert(4, 'From Different County', data['B07003010'])
        curated_data.insert(5, 'From Different State', data['B07003013'])
        curated_data.insert(6, 'From Abroad', data['B07003016'])

    elif topic_num == 15: # Population by minimum level of education
        curated_data.insert(2, 'No Degree', data['B15002003'] + \
            data['B15002004'] + data['B15002005'] + data['B15002006'] + \
            data['B15002007'] + data['B15002008'] + data['B15002009'] + \
            data['B15002010'] + data['B15002020'] + data['B15002021'] + \
            data['B15002022'] + data['B15002023'] + data['B15002024'] + \
            data['B15002025'] + data['B15002026'] + data['B15002027'])
        curated_data.insert(3, 'High School', data['B15002011'] + data['B15002028'])
        curated_data.insert(4, 'Some college', data['B15002012'] + \
            data['B15002013'] + data['B15002014'] + data['B15002029'] + \
            data['B15002030'] + data['B15002031'])
        curated_data.insert(5, 'Bachelor\'s', data['B15002015'] + data['B15002032'])
        curated_data.insert(6, 'Post-grad', data['B15002016'] + \
            data['B15002017'] + data['B15002018'] + data['B15002033'] + \
            data['B15002034'] + data['B15002035'])

    elif topic_num == 16: # Language at home, children 5-17
        curated_data.insert(2, 'English Only', data['B16007003'])
        curated_data.insert(3, 'Spanish', data['B16007004'])
        curated_data.insert(4, 'Indo-European', data['B16007005'])
        curated_data.insert(5, 'Asian/Islander', data['B16007006'])
        curated_data.insert(6, 'Other', data['B16007007'])

    elif topic_num == 17: # Language at home, adults 18+
        curated_data.insert(2, 'English Only', data['B16007009'] + data['B16007015'])
        curated_data.insert(3, 'Spanish', data['B16007010'] + data['B16007016'])
        curated_data.insert(4, 'Indo-European', data['B16007011'] + data['B16007017'])
        curated_data.insert(5, 'Asian/Islander', data['B16007012'] + data['B16007018'])
        curated_data.insert(6, 'Other', data['B16007013'] + data['B16007019'])

    elif topic_num == 18: # Place of birth for foreign-born population
        curated_data.insert(2, 'Europe', data['B05006002'])
        curated_data.insert(3, 'Asia', data['B05006047'])
        curated_data.insert(4, 'Africa', data['B05006091'])
        curated_data.insert(5, 'Oceania', data['B05006116'])
        curated_data.insert(6, 'Latin America', data['B05006123'])
        curated_data.insert(7, 'North America', data['B05006159'])

    elif topic_num == 19: # Veterans by wartime service
        curated_data.insert(2, 'WWII', data['B21002009'] + data['B21002011'] + data['B21002012'])
        curated_data.insert(3, 'Korea', data['B21002008'] + data['B21002009'] + data['B21002010'] + data['B21002011'])
        curated_data.insert(4, 'Vietnam', data['B21002004'] + data['B21002006'] + data['B21002007'] + data['B21002008'] + data['B21002009'])
        curated_data.insert(5, 'Gulf (1990s)', data['B21002003'] + data['B21002004'] + data['B21002005'] + data['B21002006'])
        curated_data.insert(6, 'Gulf (2001-)', data['B21002002'] + data['B21002003'] + data['B21002004'])

    curated_data.reset_index(drop=True, inplace=True)

    return curated_data


# TODO make this fetch(), not render().
def render(table, params):
    topic_num = int(params['topic'])
    topic = TOPIC_KEYS[topic_num]

    sumlevel_num = int(params['sumlevel'])
    if sumlevel_num == 0:
        geo = "040%7C01000US" # all states
    elif sumlevel_num == 1: # Counties
        state_selected = int(params['states-for-counties'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "050%7C04000US" + selected_state_fips
    elif sumlevel_num == 2: # Places
        state_selected = int(params['states-for-places'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "160%7C04000US" + selected_state_fips
    elif sumlevel_num == 3: # Metro Areas
        state_selected = int(params['states-for-metro-areas'])
        selected_state_fips = STATE_FIPS[state_selected]
        geo = "310%7C04000US" + selected_state_fips


    # return get_dataframe(topic, geo, geo_names=True, col_names=True)
    return get_dataframe_simple(topic_num, topic, geo)


if __name__ == "__main__":
    dframe = render(None, {'topic': 16, 'sumlevel': 1, 'states-for-counties': 0})
    print(dframe)
