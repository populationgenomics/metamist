import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from google.cloud import secretmanager

from cpg_utils.cloud import get_google_identity_token


METAMIST_CONFIG_PATH = Path.home() / '.config' / 'metamist'


@dataclass
class MetamistConfig:
    """Configuration for metamist python package"""

    url: str
    env: str
    audience: str
    desktop_oauth_creds_secret: str
    _desktop_creds: dict[str, str] | None = field(default=None, repr=False)

    def get_desktop_oauth_creds(self, do_secret_fetch: bool = False) -> dict[str, str]:
        """
        Fetch desktop oauth credentials from local file or secret manager.
        """
        if self._desktop_creds:
            return self._desktop_creds

        creds_path = METAMIST_CONFIG_PATH / self.env / 'desktop_oauth_credentials.json'
        if creds_path.exists():
            with creds_path.open('r') as f:
                self._desktop_creds = json.load(f)
                return self._desktop_creds or {}

        if not self.desktop_oauth_creds_secret:
            return {}

        # Only fetch secret if explicitly specified to avoid unnecessary calls to
        # secret manager for flows that don't need the desktop oauth creds
        if do_secret_fetch:
            secret_manager = secretmanager.SecretManagerServiceClient()

            resp = secret_manager.access_secret_version(
                request={'name': f'{self.desktop_oauth_creds_secret}/versions/latest'}
            )
            secret_value = resp.payload.data.decode('UTF-8')
            creds = json.loads(secret_value)

            creds_path.parent.mkdir(parents=True, exist_ok=True)
            with creds_path.open('w') as f:
                json.dump(creds, f)
            self._desktop_creds = creds

        return self._desktop_creds or {}

    def get_google_identity_token(self):

        try:
            return get_google_identity_token(
                target_audience=self.audience,
                enable_desktop_auth=True,
                desktop_client_id=self.desktop_client_id,
                desktop_client_secret=self.desktop_client_secret,
            )
        except ValueError as e:
            if 'using user credentials' in str(e) and 'desktop_client_secret' in str(e):
                raise RuntimeError(
                    'Using user credentials but metamist auth has not been initialised. Run `metamist auth init` to set up metamist authentication.'
                ) from e

            raise

    @property
    def desktop_client_id(self) -> str | None:
        """Get the desktop oauth client id"""
        return self.get_desktop_oauth_creds().get('client_id')

    @property
    def desktop_client_secret(self) -> str | None:
        """Get the desktop oauth client secret"""
        return self.get_desktop_oauth_creds().get('client_secret')


DEV_CONFIG = MetamistConfig(
    url='https://metamist-dev.populationgenomics.org.au',
    env='development',
    audience='113399547094-vchnsnet9lqp66hoq4l09avkil79khl6.apps.googleusercontent.com',
    desktop_oauth_creds_secret='projects/113399547094/secrets/metamist-dev-python-client-desktop-credentials',
)

PROD_CONFIG = MetamistConfig(
    url='https://metamist.populationgenomics.org.au',
    env='production',
    audience='313488420322-5l74h33pn88m9hl9t9ffvoqe6vs62lg2.apps.googleusercontent.com',
    desktop_oauth_creds_secret='projects/313488420322/secrets/metamist-python-client-desktop-credentials',
)

LOCAL_CONFIG = MetamistConfig(
    url='http://localhost:8000',
    env='local',
    audience='',
    desktop_oauth_creds_secret='',
)


def get_config() -> MetamistConfig:
    """
    Fetch metamist python client config

    Allows overriding with env vars
    """
    env = os.getenv('SM_ENVIRONMENT', 'PRODUCTION').lower()
    if 'local' in env:
        config = LOCAL_CONFIG
    elif 'dev' in env:
        config = DEV_CONFIG
    else:
        config = PROD_CONFIG

    return MetamistConfig(
        url=os.getenv('SM_URL', config.url),
        env=env,
        audience=os.getenv('SM_OAUTH_AUDIENCE', config.audience),
        desktop_oauth_creds_secret=config.desktop_oauth_creds_secret,
    )
