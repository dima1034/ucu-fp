import unittest
from unittest.mock import patch, MagicMock
from rx import from_iterable
from rx import operators as ops
from collections import defaultdict

# Import the necessary functions and classes from your code
from tracker import GithubEvent, KeywordRepoNamePair, filter_new_repos, process_search_results

class TestGithubTracking(unittest.TestCase):
    @patch('your_module.search_github')
    def test_new_repos_filtering(self, mock_search_github):
        # Mock the search_github function to return a fixed set of results
        mock_search_github.side_effect = [
            {'items': [{'repository': {'full_name': f'repo{i}'}} for i in range(30)]},
            {'items': [{'repository': {'full_name': 'repo30'}}] + [{'repository': {'full_name': f'repo{i}'}} for i in range(29)]},
        ]

        keyword = 'test'
        new_repos_storage = defaultdict(set)

        # First query
        results1 = mock_search_github(keyword)
        events1 = process_search_results(keyword, results1)
        observable1 = from_iterable(events1)
        new_repos_observable1 = observable1.pipe(filter_new_repos(new_repos_storage))

        # Second query
        results2 = mock_search_github(keyword)
        events2 = process_search_results(keyword, results2)
        observable2 = from_iterable(events2)
        new_repos_observable2 = observable2.pipe(filter_new_repos(new_repos_storage))

        # Count the number of new repos emitted by the observables
        new_repos_count1 = 0
        new_repos_count2 = 0

        def on_next1(event):
            nonlocal new_repos_count1
            new_repos_count1 += 1

        def on_next2(event):
            nonlocal new_repos_count2
            new_repos_count2 += 1

        new_repos_observable1.subscribe(on_next=on_next1)
        new_repos_observable2.subscribe(on_next=on_next2)

        # Assert the expected behavior
        self.assertEqual(new_repos_count1, 30)
        self.assertEqual(new_repos_count2, 1)

if __name__ == '__main__':
    unittest.main()