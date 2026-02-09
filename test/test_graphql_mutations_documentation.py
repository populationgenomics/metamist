"""
Test GraphQL mutations from documentation file.

This test file parses the GraphQL mutations documentation markdown file
and automatically generates tests for each mutation example, including
running the queries with the provided variables.
"""

import json
from pathlib import Path
from typing import Any

import mistune
import pytest

from test.conftest import GraphQLQueryFunction


# Path to the GraphQL mutations documentation
DOCS_PATH = Path(__file__).parent.parent / 'docs' / 'graphql_mutations.md'


def parse_graphql_mutations_from_markdown(
    markdown_path: Path,
) -> list[dict[str, Any]]:
    """
    Parse GraphQL mutation examples from the markdown documentation.

    Returns a list of dictionaries, each containing:
    - name: Name of the mutation (extracted from heading)
    - query: The GraphQL mutation query
    - variables: The JSON variables for the mutation
    """
    with markdown_path.open('r') as f:
        content = f.read()

    # Parse markdown into AST
    markdown = mistune.create_markdown(renderer=None)
    tokens = markdown(content)

    mutations = []
    current_heading = None
    current_query = None
    current_variables = None
    in_variables_section = False

    # Walk through tokens to find h3 headings followed by code blocks
    for i, token in enumerate(tokens):
        if token['type'] == 'heading' and token['attrs']['level'] == 3:
            # Save previous mutation if we have one
            if current_heading and current_query:
                mutations.append(
                    {
                        'name': current_heading,
                        'query': current_query,
                        'variables': current_variables,
                    }
                )

            # Start new mutation section
            current_heading = token['children'][0]['raw']
            current_query = None
            current_variables = None
            in_variables_section = False

        elif token['type'] == 'block_code' and current_heading:
            # Check if this is a GraphQL query block
            if token.get('attrs', {}).get('info') == 'graphql':
                current_query = token['raw'].strip()

            # Check if this is a JSON variables block
            elif token.get('attrs', {}).get('info') == 'json':
                # Check if the previous token mentions "Variables:"
                if i > 0:
                    prev_token = tokens[i - 1]
                    if prev_token['type'] == 'paragraph':
                        # Check if paragraph contains "Variables:"
                        for child in prev_token.get('children', []):
                            if child.get(
                                'type'
                            ) == 'text' and 'Variables:' in child.get('raw', ''):
                                in_variables_section = True
                                break

                if in_variables_section:
                    try:
                        current_variables = json.loads(token['raw'].strip())
                    except json.JSONDecodeError:
                        # Skip invalid JSON
                        pass
                    in_variables_section = False

    # Save last mutation if exists
    if current_heading and current_query:
        mutations.append(
            {
                'name': current_heading,
                'query': current_query,
                'variables': current_variables,
            }
        )

    return mutations


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """
    Pytest hook to parametrize tests based on the mutations in the documentation.

    This dynamically generates test cases for each mutation found in the
    graphql_mutations.md file.
    """
    if 'mutation_example' in metafunc.fixturenames:
        mutations = parse_graphql_mutations_from_markdown(DOCS_PATH)

        # Create test IDs from mutation names
        test_ids = [m['name'].lower().replace(' ', '_') for m in mutations]

        metafunc.parametrize(
            'mutation_example',
            mutations,
            ids=test_ids,
        )


class TestGraphQLMutationsDocumentation:
    """Test GraphQL mutations from the documentation file."""

    @pytest.mark.asyncio
    @pytest.mark.admin_groups(['project-creators'])
    async def test_mutation_syntax_is_valid(
        self,
        mutation_example: dict[str, Any],
        graphql_query: GraphQLQueryFunction,
    ) -> None:
        """
        Test that each mutation in the documentation has valid syntax and can be executed.

        This test verifies:
        1. The GraphQL query syntax is valid
        2. The variables (if provided) are valid JSON
        3. The query can be sent to the GraphQL endpoint
        4. The response is properly formatted (data or errors)

        Note: Some mutations may fail with application errors (e.g., duplicate entries,
        missing dependencies), but this test focuses on syntax validation and ensuring
        the documentation examples are structurally correct.
        """
        query = mutation_example['query']
        variables = mutation_example['variables']

        # Execute the GraphQL query
        result = await graphql_query(query, variables)

        # Verify response structure - should have either 'data' or 'errors'
        assert 'data' in result and 'errors' not in result, (
            f'Invalid GraphQL response structure for {mutation_example["name"]}'
        )

        # If there are errors, check they are properly formatted GraphQL errors
        if 'errors' in result:
            assert isinstance(result['errors'], list)
            for error in result['errors']:
                assert 'message' in error, 'GraphQL errors must have a message field'

        # If successful, verify data structure exists
        if 'data' in result and result['data'] is not None:
            # For mutations, the data should contain a top-level field
            # (though we don't enforce what it is since mutations vary)
            assert isinstance(result['data'], dict), 'GraphQL data must be a dictionary'
