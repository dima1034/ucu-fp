import unittest
from unittest.mock import patch, MagicMock
import asyncio

from src.github_client import _get_authorization_headers, _fetch_github_api_async, _get_languages, _fetch_data_as_async_generator

class TestGithubClient(unittest.TestCase):
    def setUp(self):
        self.keyword = "asyncio"
        self.languages_url = "https://api.github.com/repos/example/example/languages"
        self.api_url = f"https://api.github.com/search/code?q={self.keyword}"

    @patch('os.getenv')
    async def test_get_authorization_headers(self, mock_getenv):
        mock_getenv.return_value = "test_token"
        expected_headers = {
            "Authorization": "Bearer test_token",
            "Accept": "application/vnd.github.v3+json"
        }
        headers = await _get_authorization_headers()
        self.assertEqual(headers, expected_headers)
        mock_getenv.assert_called_once_with('ACCESS_TOKEN')

    @patch('aiohttp.ClientSession.get')
    async def test_fetch_github_api(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = asyncio.coroutine(lambda: {'data': 'value'})
        mock_get.return_value.__aenter__.return_value = mock_resp
        
        response = await _fetch_github_api_async(self.api_url)
        self.assertEqual(response, {'data': 'value'})
        mock_get.assert_called_once()

    @patch('src._fetch_github_api_async')
    async def test_get_languages(self, mock_fetch_api):
        mock_fetch_api.return_value = asyncio.coroutine(lambda: {'Python': 100, 'JavaScript': 50})
        languages = await _get_languages(self.languages_url)
        self.assertEqual(languages, ['Python', 'JavaScript'])
        mock_fetch_api.assert_called_once_with(self.languages_url)

    @patch('src._fetch_github_api_async')
    async def test_fetch_data_as_async_generator(self, mock_fetch_api):
        mock_fetch_api.side_effect = [
            asyncio.coroutine(lambda: {
                "items": [{
                    "repository": {
                        "full_name": "example/repo",
                        "languages_url": self.languages_url
                    }
                }]
            }),
            asyncio.coroutine(lambda: {'Python': 100})
        ]

        async def consume_generator(gen):
            return [item async for item in gen]

        events = await consume_generator(_fetch_data_as_async_generator(self.keyword))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].repo_fullname, 'example/repo')

if __name__ == '__main__':
    unittest.main()
