from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Optional, Union

from dagster import Component, ComponentLoadContext, Resolvable
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.asset_spec import (
    SYSTEM_METADATA_KEY_AUTO_CREATED_STUB_ASSET,
    AssetSpec,
)
from dagster._core.definitions.definitions_class import Definitions
from dagster.components.component_scaffolding import scaffold_component
from dagster.components.core.defs_module import DefsFolderComponent, find_components_from_context
from dagster.components.resolved.base import resolve_fields
from dagster.components.resolved.context import ResolutionContext
from dagster.components.resolved.core_models import (
    AssetPostProcessor,
    ResolvedAssetKey,
    ResolvedAssetSpec,
)
from dagster.components.resolved.model import Resolver
from dagster.components.scaffold.scaffold import Scaffolder, ScaffoldRequest, scaffold_with
from pydantic import BaseModel
from typing_extensions import TypeAlias

import dagster_airlift.core as dg_airlift_core
from dagster_airlift.core.airflow_instance import AirflowAuthBackend
from dagster_airlift.core.basic_auth import AirflowBasicAuthBackend
from dagster_airlift.core.filter import AirflowFilter
from dagster_airlift.core.load_defs import build_job_based_airflow_defs
from dagster_airlift.core.serialization.serialized_data import DagHandle, TaskHandle


@dataclass
class ResolvedAirflowBasicAuthBackend(Resolvable):
    type: Literal["basic_auth"]
    webserver_url: str
    username: str
    password: str


@dataclass
class ResolvedAirflowMwaaAuthBackend(Resolvable):
    type: Literal["mwaa"]
    env_name: str
    region_name: Optional[str] = None
    profile_name: Optional[str] = None
    aws_account_id: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None


@dataclass
class InAirflowAsset(Resolvable):
    spec: ResolvedAssetSpec


@dataclass
class InDagsterAssetRef(Resolvable):
    by_key: ResolvedAssetKey


def resolve_mapped_asset(context: ResolutionContext, model) -> Union[AssetKey, AssetSpec]:
    if isinstance(model, InAirflowAsset.model()):
        return InAirflowAsset.resolve_from_model(context, model).spec
    elif isinstance(model, InDagsterAssetRef.model()):
        return InDagsterAssetRef.resolve_from_model(context, model).by_key
    else:
        raise ValueError(f"Unsupported asset type: {type(model)}")


ResolvedMappedAsset: TypeAlias = Annotated[
    Union[AssetKey, AssetSpec],
    Resolver(
        resolve_mapped_asset,
        model_field_type=Union[InAirflowAsset.model(), InDagsterAssetRef.model()],
    ),
]


@dataclass
class AirflowFilterParams(Resolvable):
    dag_id_ilike: Optional[str] = None
    airflow_tags: Optional[Sequence[str]] = None
    retrieve_datasets: bool = True
    dataset_uri_ilike: Optional[str] = None


def resolve_airflow_filter(context: ResolutionContext, model) -> AirflowFilter:
    return AirflowFilter(**resolve_fields(model, AirflowFilterParams, context))


ResolvedAirflowFilter: TypeAlias = Annotated[
    AirflowFilter,
    Resolver(
        resolve_airflow_filter,
        model_field_type=AirflowFilterParams.model(),
    ),
]


@dataclass
class AirflowTaskMapping(Resolvable):
    task_id: str
    assets: Sequence[ResolvedMappedAsset]


@dataclass
class AirflowDagMapping(Resolvable):
    dag_id: str
    assets: Optional[Sequence[ResolvedMappedAsset]] = None
    task_mappings: Optional[Sequence[AirflowTaskMapping]] = None


class AirflowInstanceScaffolderParams(BaseModel):
    name: str
    auth_type: Literal["basic_auth", "mwaa"]


class AirflowInstanceScaffolder(Scaffolder[AirflowInstanceScaffolderParams]):
    @classmethod
    def get_scaffold_params(cls) -> type[AirflowInstanceScaffolderParams]:
        return AirflowInstanceScaffolderParams

    def scaffold(self, request: ScaffoldRequest[AirflowInstanceScaffolderParams]) -> None:
        full_params: dict[str, Any] = {
            "name": request.params.name,
        }
        if request.params.auth_type == "basic_auth":
            full_params["auth"] = {
                "type": "basic_auth",
                "webserver_url": '{{ env("AIRFLOW_WEBSERVER_URL") }}',
                "username": '{{ env("AIRFLOW_USERNAME") }}',
                "password": '{{ env("AIRFLOW_PASSWORD") }}',
            }
        else:
            raise ValueError(f"Unsupported auth type: {request.params.auth_type}")
        scaffold_component(request, full_params)


def resolve_auth(context: ResolutionContext, auth) -> AirflowAuthBackend:
    if auth.type == "basic_auth":
        resolved_model_fields = resolve_fields(auth, ResolvedAirflowBasicAuthBackend, context)
        return AirflowBasicAuthBackend(
            webserver_url=resolved_model_fields["webserver_url"],
            username=resolved_model_fields["username"],
            password=resolved_model_fields["password"],
        )
    elif auth.type == "mwaa":
        resolved_model_fields = resolve_fields(auth, ResolvedAirflowMwaaAuthBackend, context)
        try:
            from dagster_airlift.mwaa import MwaaSessionAuthBackend
        except ImportError:
            raise ValueError(
                "When resolving an AirflowInstance with MWAA auth, dagster-airlift.mwaa was not installed. Please install the `dagster-airlift[mwaa]` subpackage."
            )
        try:
            import boto3
        except ImportError:
            raise ValueError(
                "When resolving an AirflowInstance with MWAA auth, boto3 was not installed. Please ensure boto3 is installed in your environment."
            )
        boto_session = boto3.Session(
            region_name=resolved_model_fields.get("region_name"),
            profile_name=resolved_model_fields.get("profile_name"),
            aws_access_key_id=resolved_model_fields.get("aws_access_key_id"),
            aws_secret_access_key=resolved_model_fields.get("aws_secret_access_key"),
            aws_session_token=resolved_model_fields.get("aws_session_token"),
            aws_account_id=resolved_model_fields.get("aws_account_id"),
        )
        return MwaaSessionAuthBackend(
            mwaa_client=boto_session.client("mwaa"),
            env_name=resolved_model_fields["env_name"],
        )
    else:
        raise ValueError(f"Unsupported auth type: {auth.type}")


ResolvedAirflowAuthBackend: TypeAlias = Annotated[
    AirflowAuthBackend,
    Resolver(
        resolve_auth,
        model_field_type=Union[
            ResolvedAirflowBasicAuthBackend.model(), ResolvedAirflowMwaaAuthBackend.model()
        ],
    ),
]


@scaffold_with(AirflowInstanceScaffolder)
@dataclass
class AirflowInstanceComponent(Component, Resolvable):
    auth: ResolvedAirflowAuthBackend
    name: str
    filter: Optional[ResolvedAirflowFilter] = None
    mappings: Optional[Sequence[AirflowDagMapping]] = None
    source_code_retrieval_enabled: Optional[bool] = None
    # TODO: deprecate and then delete -- schrockn 2025-06-10
    asset_post_processors: Optional[Sequence[AssetPostProcessor]] = None

    def _get_instance(self) -> dg_airlift_core.AirflowInstance:
        return dg_airlift_core.AirflowInstance(
            auth_backend=self.auth,
            name=self.name,
        )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        defs = build_job_based_airflow_defs(
            airflow_instance=self._get_instance(),
            mapped_defs=apply_mappings(defs_from_subdirs(context), self.mappings or []),
            source_code_retrieval_enabled=self.source_code_retrieval_enabled,
            retrieval_filter=self.filter or AirflowFilter(),
        )
        for post_processor in self.asset_post_processors or []:
            defs = post_processor(defs)
        return defs


def defs_from_subdirs(context: ComponentLoadContext) -> Definitions:
    """Get all definitions coming from subdirectories in the current context. We explicitly don't want to include the current directory in the search, to avoid including the current component."""
    return DefsFolderComponent(
        path=context.path,
        children=find_components_from_context(context),
    ).build_defs(context)


def handle_iterator(
    mappings: Optional[Sequence[AirflowDagMapping]],
) -> Iterator[tuple[Union[TaskHandle, DagHandle], Sequence[ResolvedMappedAsset]]]:
    if mappings is None:
        return
    for mapping in mappings:
        for task_mapping in mapping.task_mappings or []:
            yield (
                TaskHandle(dag_id=mapping.dag_id, task_id=task_mapping.task_id),
                task_mapping.assets,
            )
        yield DagHandle(dag_id=mapping.dag_id), mapping.assets or []


def apply_mappings(defs: Definitions, mappings: Sequence[AirflowDagMapping]) -> Definitions:
    specs = {spec.key: spec for spec in defs.resolve_all_asset_specs()}
    key_to_handle_mapping = {}
    additional_assets = []

    for handle, asset_refs in handle_iterator(mappings):
        if not asset_refs:
            continue
        for asset in asset_refs:
            if isinstance(asset, AssetKey):
                if asset not in specs:
                    raise ValueError(f"Asset with key {asset} not found in definitions")
                key_to_handle_mapping[asset] = handle
            elif isinstance(asset, AssetSpec):
                if asset.key in specs and not is_autocreated_stub_asset(specs[asset.key]):
                    raise ValueError(f"Asset with key {asset.key} already exists in definitions")
                additional_assets.append(
                    asset.merge_attributes(metadata={handle.metadata_key: [handle.as_dict]})
                )

    def spec_mapper(spec: AssetSpec) -> AssetSpec:
        if spec.key in key_to_handle_mapping:
            handle = key_to_handle_mapping[spec.key]
            return spec.merge_attributes(metadata={handle.metadata_key: [handle.as_dict]})
        return spec

    return Definitions.merge(
        defs.map_resolved_asset_specs(func=spec_mapper), Definitions(assets=additional_assets)
    )


def is_autocreated_stub_asset(spec: AssetSpec) -> bool:
    return spec.metadata.get(SYSTEM_METADATA_KEY_AUTO_CREATED_STUB_ASSET) is True
