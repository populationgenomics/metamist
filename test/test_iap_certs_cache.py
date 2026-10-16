from http import HTTPStatus
from typing import cast
from unittest.mock import MagicMock

import pytest
from google.auth.exceptions import InvalidValue, MalformedError, TransportError
from requests import exceptions as requests_exceptions
from requests.sessions import Session

from api.utils import db
from api.utils.db import IAP_CERTS_URL, CachingCertsRequest


CERTS_BODY = b'{"a-key-id": "a-pem-encoded-public-key"}'
ROTATED_CERTS_BODY = b'{"another-key-id": "another-pem-encoded-public-key"}'


def mock_response(body: bytes = CERTS_BODY, status: int = HTTPStatus.OK) -> MagicMock:
    """A stand-in for the requests.Response that the session would return"""
    response = MagicMock()
    response.status_code = status
    response.content = body
    return response


def mock_request(
    *results: MagicMock | Exception,
) -> tuple[CachingCertsRequest, MagicMock]:
    """
    A CachingCertsRequest, and the mock session backing it, which returns (or raises)
    the given results in order instead of making network calls
    """
    session = MagicMock()
    session.request.side_effect = list(results)

    request = CachingCertsRequest()
    request.session = cast(Session, session)
    return request, session


class TestCachingCertsRequest:
    """Tests for the transport used to fetch the IAP signing keys"""

    def test_keys_are_only_fetched_once(self) -> None:
        """
        google-auth otherwise refetches on every verify_token() call, which
        put a TLS handshake in front of every authenticated request
        """
        request, session = mock_request(mock_response())

        assert request(IAP_CERTS_URL).data == CERTS_BODY
        assert request(IAP_CERTS_URL).data == CERTS_BODY

        assert session.request.call_count == 1

    def test_keys_are_refetched_once_the_cache_expires(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rotated signing keys should still get picked up"""
        request, _ = mock_request(mock_response(), mock_response(ROTATED_CERTS_BODY))
        # negative, so that anything cached is already past its ttl
        monkeypatch.setattr('api.utils.db.IAP_CERTS_CACHE_SECONDS', -1)

        assert request(IAP_CERTS_URL).data == CERTS_BODY
        assert request(IAP_CERTS_URL).data == ROTATED_CERTS_BODY

    def test_refresh_picks_up_rotated_keys_before_the_cache_expires(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        A refresh is what lets a verification failure recover from a key rotation
        without waiting out the full cache ttl
        """
        request, _ = mock_request(mock_response(), mock_response(ROTATED_CERTS_BODY))
        # negative, so that the refresh is never considered too soon
        monkeypatch.setattr('api.utils.db.IAP_CERTS_REFRESH_SECONDS', -1)

        assert request(IAP_CERTS_URL).data == CERTS_BODY
        request.refresh(IAP_CERTS_URL)

        assert request(IAP_CERTS_URL).data == ROTATED_CERTS_BODY

    def test_refresh_is_rate_limited(self) -> None:
        """
        The key id comes from an unauthenticated header, so bogus tokens shouldn't be
        able to make us fetch certs on every request
        """
        request, session = mock_request(mock_response())

        request(IAP_CERTS_URL)
        for _ in range(5):
            request.refresh(IAP_CERTS_URL)

        assert session.request.call_count == 1

    def test_error_responses_are_not_cached(self) -> None:
        """
        A 4xx isn't retried by the session, so if it were cached it would break
        authentication for the whole ttl
        """
        request, session = mock_request(
            mock_response(b'nope', status=HTTPStatus.FORBIDDEN), mock_response()
        )

        assert request(IAP_CERTS_URL).status == HTTPStatus.FORBIDDEN
        assert request(IAP_CERTS_URL).data == CERTS_BODY
        assert session.request.call_count == 2

    def test_fetch_errors_are_raised(self) -> None:
        """google-auth wraps the underlying requests error"""
        request, _ = mock_request(
            requests_exceptions.ConnectionError('endpoint is down')
        )

        with pytest.raises(TransportError):
            request(IAP_CERTS_URL)


class TestValidateIapJwt:
    """Tests for the retry that recovers from a key rotation"""

    def test_verification_is_retried_after_refreshing_the_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        A MalformedError is what google-auth raises when the token was signed with a
        key that isn't in the certs we hold
        """
        verify_token = MagicMock(
            side_effect=[
                MalformedError('Certificate for key id a-key-id not found.'),
                {'email': 'someone@popgen.org.au'},
            ]
        )
        refresh = MagicMock()
        monkeypatch.setattr(db.id_token, 'verify_token', verify_token)
        monkeypatch.setattr(db.iap_certs_request, 'refresh', refresh)

        email = db.validate_iap_jwt_and_get_email('a-jwt', 'an-audience')

        assert email == 'someone@popgen.org.au'
        refresh.assert_called_once_with(IAP_CERTS_URL)
        assert verify_token.call_count == 2

    def test_verification_is_only_retried_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A genuinely invalid token shouldn't keep getting another go"""
        verify_token = MagicMock(side_effect=MalformedError('nope'))
        monkeypatch.setattr(db.id_token, 'verify_token', verify_token)
        monkeypatch.setattr(db.iap_certs_request, 'refresh', MagicMock())

        with pytest.raises(MalformedError):
            db.validate_iap_jwt_and_get_email('a-jwt', 'an-audience')

        assert verify_token.call_count == 2

    def test_other_validation_failures_do_not_refresh_the_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An expired token or a wrong audience has nothing to do with the certs"""
        verify_token = MagicMock(side_effect=InvalidValue('Token expired'))
        refresh = MagicMock()
        monkeypatch.setattr(db.id_token, 'verify_token', verify_token)
        monkeypatch.setattr(db.iap_certs_request, 'refresh', refresh)

        with pytest.raises(InvalidValue):
            db.validate_iap_jwt_and_get_email('a-jwt', 'an-audience')

        refresh.assert_not_called()
        assert verify_token.call_count == 1
