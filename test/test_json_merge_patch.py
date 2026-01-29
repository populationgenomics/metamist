from typing import Awaitable, Callable

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool


@pytest_asyncio.fixture
async def json_merge_patch(
    db_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
):
    """Helper fixture to call the json_merge_patch function."""

    async def _merge(target: str, patch: str) -> str:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    'SELECT json_merge_patch(%s::jsonb, %s::jsonb)::text AS result',
                    (target, patch),
                )
                row = await cur.fetchone()
                assert row is not None
                result = row.get('result')
                assert result is not None
                return result

    return _merge


class TestRFC7396Compliance:
    """
    Test cases from RFC 7396 Appendix A.
    https://datatracker.ietf.org/doc/html/rfc7396#appendix-A

    These are the official examples provided in the specification.
    """

    @pytest.mark.asyncio
    async def test_example_1(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 1."""
        result = await json_merge_patch('{"a":"b"}', '{"a":"c"}')
        assert result == '{"a": "c"}'

    @pytest.mark.asyncio
    async def test_example_2(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 2."""
        result = await json_merge_patch('{"a":"b"}', '{"b":"c"}')
        assert result == '{"a": "b", "b": "c"}'

    @pytest.mark.asyncio
    async def test_example_3(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 3."""
        result = await json_merge_patch('{"a":"b"}', '{"a":null}')
        assert result == '{}'

    @pytest.mark.asyncio
    async def test_example_4(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 4."""
        result = await json_merge_patch('{"a":"b", "b":"c"}', '{"a":null}')
        assert result == '{"b": "c"}'

    @pytest.mark.asyncio
    async def test_example_5(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 5."""
        result = await json_merge_patch('{"a":["b"]}', '{"a":"c"}')
        assert result == '{"a": "c"}'

    @pytest.mark.asyncio
    async def test_example_6(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 6."""
        result = await json_merge_patch('{"a":"c"}', '{"a":["b"]}')
        assert result == '{"a": ["b"]}'

    @pytest.mark.asyncio
    async def test_example_7(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 7."""
        result = await json_merge_patch(
            '{"a": {"b": "c"}}', '{"a": {"b": "d", "c": null}}'
        )
        assert result == '{"a": {"b": "d"}}'

    @pytest.mark.asyncio
    async def test_example_8(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 8."""
        result = await json_merge_patch('{"a": [{"b":"c"}]}', '{"a": [1]}')
        assert result == '{"a": [1]}'

    @pytest.mark.asyncio
    async def test_example_9(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 9."""
        result = await json_merge_patch('["a","b"]', '["c","d"]')
        assert result == '["c", "d"]'

    @pytest.mark.asyncio
    async def test_example_10(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 10."""
        result = await json_merge_patch('{"a":"b"}', '["c"]')
        assert result == '["c"]'

    @pytest.mark.asyncio
    async def test_example_11(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 11."""
        result = await json_merge_patch('{"a":"foo"}', 'null')
        assert result == 'null'

    @pytest.mark.asyncio
    async def test_example_12(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 12."""
        result = await json_merge_patch('{"a":"foo"}', '"bar"')
        assert result == '"bar"'

    @pytest.mark.asyncio
    async def test_example_13(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 13."""
        result = await json_merge_patch('{"e":null}', '{"a":1}')
        # The key order changes here due to the way JSONB stores data, this is an
        # acceptable variation from the spec
        assert result == '{"a": 1, "e": null}'

    @pytest.mark.asyncio
    async def test_example_14(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 14."""
        result = await json_merge_patch('[1,2]', '{"a": "b", "c": null}')
        assert result == '{"a": "b"}'

    @pytest.mark.asyncio
    async def test_example_15(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Example 15."""
        result = await json_merge_patch('{}', '{"a": {"bb": {"ccc": null}}}')
        assert result == '{"a": {"bb": {}}}'


class TestEdgeCases:
    """Additional test cases for things not covered in the RFC examples."""

    @pytest.mark.asyncio
    async def test_empty_patch(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Empty patch leaves target unchanged."""
        result = await json_merge_patch('{"a":1,"b":2}', '{}')
        assert result == '{"a": 1, "b": 2}'

    @pytest.mark.asyncio
    async def test_deeply_nested_merge(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Deeply nested object merge."""
        result = await json_merge_patch(
            '{"a":{"b":{"c":{"d":1}}}}',
            '{"a":{"b":{"c":{"e":2}}}}',
        )
        assert result == '{"a": {"b": {"c": {"d": 1, "e": 2}}}}'

    @pytest.mark.asyncio
    async def test_deeply_nested_removal(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Removing deeply nested key."""
        result = await json_merge_patch(
            '{"a":{"b":{"c":1,"d":2}}}',
            '{"a":{"b":{"c":null}}}',
        )
        assert result == '{"a": {"b": {"d": 2}}}'

    @pytest.mark.asyncio
    async def test_boolean_values(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling boolean values."""
        result = await json_merge_patch(
            '{"enabled":true}',
            '{"enabled":false}',
        )
        assert result == '{"enabled": false}'

    @pytest.mark.asyncio
    async def test_numeric_values(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling numeric values."""
        result = await json_merge_patch(
            '{"count":1}',
            '{"count":42}',
        )
        assert result == '{"count": 42}'

    @pytest.mark.asyncio
    async def test_float_values(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling float values."""
        result = await json_merge_patch(
            '{"value":1.5}',
            '{"value":2.7}',
        )
        assert result == '{"value": 2.7}'

    @pytest.mark.asyncio
    async def test_mixed_type_replacement(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Replacing value with different type."""
        result = await json_merge_patch(
            '{"value":"string"}',
            '{"value":123}',
        )
        assert result == '{"value": 123}'

    @pytest.mark.asyncio
    async def test_array_not_merged(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Arrays are replaced, not merged (important RFC requirement)."""
        result = await json_merge_patch(
            '{"items":[1,2,3]}',
            '{"items":[4,5]}',
        )
        assert result == '{"items": [4, 5]}'

    @pytest.mark.asyncio
    async def test_null_in_array_preserved(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Null values inside arrays are preserved (not special)."""
        result = await json_merge_patch(
            '{"items":[1,2]}',
            '{"items":[null,3]}',
        )
        assert result == '{"items": [null, 3]}'

    @pytest.mark.asyncio
    async def test_multiple_operations(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Multiple operations in single patch."""
        result = await json_merge_patch(
            '{"a":1,"b":2,"c":3}',
            '{"a":null,"b":20,"d":4}',
        )
        assert result == '{"b": 20, "c": 3, "d": 4}'

    @pytest.mark.asyncio
    async def test_unicode(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling unicode in keys/values."""
        result = await json_merge_patch(
            '{"🥚":"🥚"}',
            '{"🥚":"🐣"}',
        )
        assert result == '{"🥚": "🐣"}'

    @pytest.mark.asyncio
    async def test_special_characters_in_strings(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling special characters in string values."""
        result = await json_merge_patch(
            '{"text":"line1"}',
            '{"text":"line1\\nline2"}',
        )
        assert result == '{"text": "line1\\nline2"}'

    @pytest.mark.asyncio
    async def test_numeric_keys(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Handling that keys that are numbers work"""
        result = await json_merge_patch(
            '{"1":2}',
            '{"1":3}',
        )
        assert result == '{"1": 3}'

    @pytest.mark.asyncio
    async def test_null_removes_nested_key(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Null removes nested key."""
        result = await json_merge_patch(
            '{"a":{"b":1,"c":2}}',
            '{"a":{"b":null}}',
        )
        assert result == '{"a": {"c": 2}}'

    @pytest.mark.asyncio
    async def test_null_target_value(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Target with null value gets replaced."""
        result = await json_merge_patch('{"a":null}', '{"a":1}')
        assert result == '{"a": 1}'

    @pytest.mark.asyncio
    async def test_explicit_null_vs_missing(
        self, json_merge_patch: Callable[[str, str], Awaitable[str]]
    ):
        """Explicit null in target remains if not patched."""
        result = await json_merge_patch('{"a":null,"b":1}', '{"b":2}')
        assert result == '{"a": null, "b": 2}'
