# burningdemand_dagster/resources/external_apis_resource.py
from dagster import ConfigurableResource

class ExternalAPIsResource(ConfigurableResource):
    github_token: str
    anthropic_api_key: str
    pocketbase_url: str
    pocketbase_admin_token: str
