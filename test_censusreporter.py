import unittest
from censusreporter import migrate_params


class MigrateParamsTest(unittest.TestCase):
    def test_v0_places(self):
        self.assertEqual(migrate_params({
            'topic': 10,  # v0: 'Ownership of Occupied Units'
            'sumlevel': 2,  # v0: 'Places'
            'states-for-counties': 2,  # v0: ignored
            'states-for-places': 4,  # v0: 'California'
            'states-for-metro-areas': 23,  # v0: ignored
        }), {
            'topic': 'b25003', # https://censusreporter.org/tables/B25003/
            'sumlevel': 'places',
            'statecode': 'ca',
        })

    def test_v0_all_states(self):
        self.assertEqual(migrate_params({
            'topic': 3,  # v0: 'Household Income'
            'sumlevel': 0,  # v0: 'All States'
            'states-for-counties': 2,  # v0: 'Arizona'
            'states-for-places': 4,  # v0: ignored
            'states-for-metro-areas': 23,  # v0: ignored
        }), {
            'topic': 'b19001', # https://censusreporter.org/tables/B19001/
            'sumlevel': 'all_states',
            'statecode': 'az',
        })

    def test_v1(self):
        self.assertEqual(migrate_params({
            'topic': 'b19001', # https://censusreporter.org/tables/B19001/
            'sumlevel': 'all_states',
            'statecode': 'az',
        }), {
            'topic': 'b19001', # https://censusreporter.org/tables/B19001/
            'sumlevel': 'all_states',
            'statecode': 'az',
        })


if __name__ == '__main__':
    unittest.main()
