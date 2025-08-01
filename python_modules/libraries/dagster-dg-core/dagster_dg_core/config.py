import functools
import textwrap
import warnings
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

import click
from click.core import ParameterSource
from dagster_shared.match import match_type
from dagster_shared.merger import deep_merge_dicts
from dagster_shared.plus.config import load_config
from dagster_shared.seven import is_valid_module_pattern
from dagster_shared.utils import remove_none_recursively
from dagster_shared.utils.config import (
    DgConfigFileFormat,
    detect_dg_config_file_format,
    discover_config_file,
    does_dg_config_file_exist,
    get_dg_config_path,
)
from typing_extensions import Never, NotRequired, Required, Self, TypeAlias, TypeGuard

from dagster_dg_core.error import DgError, DgValidationError
from dagster_dg_core.utils import exit_with_error, get_toml_node, has_toml_node, modify_toml
from dagster_dg_core.utils.warnings import DgWarningIdentifier, emit_warning

if TYPE_CHECKING:
    import tomlkit
    import tomlkit.items

T = TypeVar("T")


def discover_workspace_root(path: Path) -> Optional[Path]:
    workspace_config_path = discover_config_file(
        path, lambda config: config["directory_type"] == "workspace"
    )
    return workspace_config_path.parent if workspace_config_path else None


# NOTE: The presence of dg.toml will cause pyproject.toml to be ignored for purposes of dg config.

_CLI_CONTEXT_CONFIG_KEY = "config"


def set_config_on_cli_context(cli_context: click.Context, config: "DgRawCliConfig") -> None:
    cli_context.ensure_object(dict)
    cli_context.obj[_CLI_CONTEXT_CONFIG_KEY] = config


def has_config_on_cli_context(cli_context: click.Context) -> bool:
    return _CLI_CONTEXT_CONFIG_KEY in cli_context.ensure_object(dict)


def get_config_from_cli_context(cli_context: click.Context) -> "DgRawCliConfig":
    cli_context.ensure_object(dict)
    return cli_context.obj[_CLI_CONTEXT_CONFIG_KEY]


# ########################
# ##### MAIN
# ########################


@dataclass
class DgConfig:
    cli: "DgCliConfig"
    project: Optional["DgProjectConfig"] = None
    workspace: Optional["DgWorkspaceConfig"] = None

    @classmethod
    def default(cls) -> Self:
        return cls(DgCliConfig.default())

    # Note that this function takes an argument called `container_workspace_file_config` instead of
    # just `workspace_file_config` because this is only to be passed when the root file config
    # points to a project inside of a workspace. If there is no package and the
    # root is itself a workspace, then `root_file_config` corresponds to the workspace and
    # `container_workspace_file_config` should be None.
    @classmethod
    def from_partial_configs(
        cls,
        root_file_config: Optional["DgFileConfig"] = None,
        container_workspace_file_config: Optional["DgWorkspaceFileConfig"] = None,
        command_line_config: Optional["DgRawCliConfig"] = None,
        user_config: Optional["DgRawCliConfig"] = None,
    ) -> Self:
        cli_partials: list[DgRawCliConfig] = []

        if container_workspace_file_config:
            if "cli" in container_workspace_file_config:
                cli_partials.append(container_workspace_file_config["cli"])
            workspace_config = DgWorkspaceConfig.from_raw(
                container_workspace_file_config["workspace"]
            )

        if root_file_config:
            if "cli" in root_file_config:
                cli_partials.append(root_file_config["cli"])

            if is_workspace_file_config(root_file_config):
                project_config = None
                workspace_config = DgWorkspaceConfig.from_raw(root_file_config["workspace"])
            elif is_project_file_config(root_file_config):
                project_config = DgProjectConfig.from_raw(root_file_config["project"])
                if not container_workspace_file_config:
                    workspace_config = None

        else:
            project_config = None
            workspace_config = None

        if command_line_config:
            cli_partials.append(command_line_config)

        all_cli_config = [user_config, *cli_partials] if user_config else cli_partials
        cli_config = (
            DgCliConfig.from_raw(*all_cli_config) if all_cli_config else DgCliConfig.default()
        )

        return cls(cli_config, project_config, workspace_config)  # pyright: ignore[reportPossiblyUnboundVariable]


# ########################
# ##### CLI
# ########################


@dataclass
class DgCliConfig:
    """CLI configuration for Dg.

    Args:
        verbose (bool): If True, log debug information.
        use_component_modules (list[str]): Specify a list of modules containing components.
            Any components retrieved from the remote environment will be filtered to only include those
            from these modules. This is useful primarily for testing, as it allows targeting of a stable
            set of test components.
    """

    verbose: bool = False
    use_component_modules: list[str] = field(default_factory=list)
    suppress_warnings: list["DgWarningIdentifier"] = field(default_factory=list)
    telemetry_enabled: bool = True

    @classmethod
    def default(cls) -> Self:
        return cls()

    @classmethod
    def from_raw(cls, *partials: "DgRawCliConfig") -> Self:
        merged = cast("DgRawCliConfig", functools.reduce(lambda acc, x: {**acc, **x}, partials))  # type: ignore
        return cls(
            verbose=merged.get("verbose", DgCliConfig.verbose),
            use_component_modules=merged.get(
                "use_component_modules",
                cls.__dataclass_fields__["use_component_modules"].default_factory(),
            ),
            suppress_warnings=merged.get(
                "suppress_warnings",
                cls.__dataclass_fields__["suppress_warnings"].default_factory(),
            ),
            telemetry_enabled=merged.get("telemetry", {}).get(
                "enabled", DgCliConfig.telemetry_enabled
            ),
        )


class RawDgTelemetryConfig(TypedDict, total=False):
    enabled: bool


# All fields are optional
class DgRawCliConfig(TypedDict, total=False):
    verbose: bool
    use_component_modules: Sequence[str]
    suppress_warnings: Sequence[DgWarningIdentifier]
    telemetry: RawDgTelemetryConfig


# ########################
# ##### PROJECT
# ########################


class DgRawBuildConfig(TypedDict):
    registry: Optional[str]
    directory: Optional[str]


def merge_build_configs(
    workspace_build_config: Optional[DgRawBuildConfig],
    project_build_config: Optional[DgRawBuildConfig],
) -> DgRawBuildConfig:
    project_dict = remove_none_recursively(project_build_config or {})
    workspace_dict = remove_none_recursively(workspace_build_config or {})
    return cast(
        "DgRawBuildConfig",
        deep_merge_dicts(workspace_dict, project_dict),
    )


def merge_container_context_configs(
    workspace_container_context_config: Optional[Mapping[str, Any]],
    project_container_context_config: Optional[Mapping[str, Any]],
) -> Mapping[str, Any]:
    # defer for import performance
    from dagster_cloud_cli.config import DagsterCloudConfigDefaultsMerger

    merger = DagsterCloudConfigDefaultsMerger()
    return merger.merge(
        {**workspace_container_context_config} if workspace_container_context_config else {},
        {**project_container_context_config} if project_container_context_config else {},
    )


@dataclass
class DgProjectConfig:
    root_module: str
    defs_module: Optional[str] = None
    autoload_defs: bool = False
    code_location_target_module: Optional[str] = None
    code_location_name: Optional[str] = None
    registry_modules: list[str] = field(default_factory=list)

    @classmethod
    def from_raw(cls, raw: "DgRawProjectConfig") -> Self:
        if raw.get("autoload_defs"):
            warnings.warn(
                "Using autoload_defs in pyproject.toml is deprecated, and will be removed in dagster 1.11.0."
                " Use a definitions.py file with @definitions and load_project_defs instead:\n"
                + textwrap.dedent(
                    """
                    from pathlib import Path

                    from dagster import definitions, load_from_defs_folder


                    @definitions
                    def defs():
                        return load_from_defs_folder(project_root=Path(__file__).parent.parent.parent)
                    """
                )
            )
        return cls(
            root_module=raw["root_module"],
            autoload_defs=raw.get("autoload_defs", DgProjectConfig.autoload_defs),
            defs_module=raw.get("defs_module", DgProjectConfig.defs_module),
            code_location_name=raw.get("code_location_name", DgProjectConfig.code_location_name),
            code_location_target_module=raw.get(
                "code_location_target_module",
                DgProjectConfig.code_location_target_module,
            ),
            registry_modules=raw.get(
                "registry_modules", cls.__dataclass_fields__["registry_modules"].default_factory()
            ),
        )


class DgRawProjectConfig(TypedDict):
    root_module: Required[str]
    autoload_defs: NotRequired[bool]
    defs_module: NotRequired[str]
    code_location_target_module: NotRequired[str]
    code_location_name: NotRequired[str]
    registry_modules: NotRequired[list[str]]


# ########################
# ##### WORKSPACE
# ########################


@dataclass
class DgWorkspaceConfig:
    projects: list["DgWorkspaceProjectSpec"]
    scaffold_project_options: "DgWorkspaceScaffoldProjectOptions"

    @classmethod
    def from_raw(cls, raw: "DgRawWorkspaceConfig") -> Self:
        projects = [DgWorkspaceProjectSpec.from_raw(spec) for spec in raw.get("projects", [])]
        scaffold_project_options = DgWorkspaceScaffoldProjectOptions.from_raw(
            raw.get("scaffold_project_options", {})
        )
        return cls(projects, scaffold_project_options)


class DgRawWorkspaceConfig(TypedDict, total=False):
    projects: list["DgRawWorkspaceProjectSpec"]
    scaffold_project_options: "DgRawWorkspaceNewProjectOptions"


@dataclass
class DgWorkspaceProjectSpec:
    path: Path

    @classmethod
    def from_raw(cls, raw: "DgRawWorkspaceProjectSpec") -> Self:
        return cls(
            path=Path(raw["path"]),
        )


class DgRawWorkspaceProjectSpec(TypedDict, total=False):
    path: Required[str]


@dataclass
class DgWorkspaceScaffoldProjectOptions:
    use_editable_dagster: Union[str, bool] = False

    @classmethod
    def from_raw(cls, raw: "DgRawWorkspaceNewProjectOptions") -> Self:
        return cls(
            use_editable_dagster=raw.get(
                "use_editable_dagster", DgWorkspaceScaffoldProjectOptions.use_editable_dagster
            ),
        )

    # This is here instead of on `DgRawWorkspaceNewProjectOptions` because TypedDict can't have
    # methods.
    @classmethod
    def get_raw_from_cli(
        cls,
        use_editable_dagster: Optional[str],
    ) -> "DgRawWorkspaceNewProjectOptions":
        raw_scaffold_project_options: DgRawWorkspaceNewProjectOptions = {}
        if use_editable_dagster:
            raw_scaffold_project_options["use_editable_dagster"] = (
                True if use_editable_dagster == "TRUE" else use_editable_dagster
            )
        return raw_scaffold_project_options


class DgRawWorkspaceNewProjectOptions(TypedDict, total=False):
    use_editable_dagster: Union[str, bool]


# ########################
# ##### CLI CONFIG
# ########################


def normalize_cli_config(
    cli_options: Mapping[str, object], cli_context: click.Context
) -> DgRawCliConfig:
    # Remove any options that weren't explicitly provided.
    filtered_options = {
        key: value
        for key, value in cli_options.items()
        if cli_context.get_parameter_source(key) != ParameterSource.DEFAULT
    }
    return _validate_cli_config(filtered_options)


def _validate_cli_config(cli_opts: Mapping[str, object]) -> DgRawCliConfig:
    try:
        for key, type_ in DgRawCliConfig.__annotations__.items():
            _validate_cli_config_setting(cli_opts, key, type_)
        _validate_cli_config_no_extraneous_keys(cli_opts)
    except DgValidationError as e:
        _raise_cli_config_validation_error(str(e))
    return cast("DgRawCliConfig", cli_opts)


def _validate_cli_config_setting(cli_opts: Mapping[str, object], key: str, type_: type) -> None:
    if key in cli_opts and not match_type(cli_opts[key], type_):
        raise DgValidationError(f"`{key}` must be a {type_.__name__}.")


def _validate_cli_config_no_extraneous_keys(cli_opts: Mapping[str, object]) -> None:
    extraneous_keys = [k for k in cli_opts.keys() if k not in DgRawCliConfig.__annotations__]
    if extraneous_keys:
        raise DgValidationError(f"Unrecognized fields:\n    {extraneous_keys}")


def _raise_cli_config_validation_error(message: str) -> None:
    raise DgError(f"Error in CLI options:\n    {message}")


# ########################
# ##### FILE CONFIG
# ########################

# The Dg*FileConfig classes wrap config extracted from a config file. This may be either a dg.toml
# file or a pyproject.toml file. For `dg.toml`, the config is defined a the top level. For
# pyproject.toml the config must be mounted on the `tool.dg` section. Either way, once the config
# has been parsed and extracted into a Dg*FileConfig class, it does not matter which file type it
# was sourced from.

DgFileConfigDirectoryType = Literal["workspace", "project"]


class DgWorkspaceFileConfig(TypedDict):
    directory_type: Required[Literal["workspace"]]
    workspace: Required[DgRawWorkspaceConfig]
    cli: NotRequired[DgRawCliConfig]


def is_workspace_file_config(config: "DgFileConfig") -> TypeGuard[DgWorkspaceFileConfig]:
    return config["directory_type"] == "workspace"


class DgProjectFileConfig(TypedDict):
    directory_type: Required[Literal["project"]]
    project: Required[DgRawProjectConfig]
    cli: NotRequired[DgRawCliConfig]


def is_project_file_config(config: "DgFileConfig") -> TypeGuard[DgProjectFileConfig]:
    return config["directory_type"] == "project"


DgFileConfig: TypeAlias = Union[DgWorkspaceFileConfig, DgProjectFileConfig]


@contextmanager
def modify_dg_toml_config(
    path: Path,
) -> Iterator[Union["tomlkit.TOMLDocument", "tomlkit.items.Table"]]:
    """Modify a TOML file as a tomlkit.TOMLDocument, preserving comments and formatting."""
    import tomlkit
    import tomlkit.items

    with modify_toml(path) as toml:
        if detect_dg_config_file_format(path) == "root":
            yield toml
        elif not has_toml_node(toml, ("tool", "dg")):
            raise KeyError(
                "TOML file does not have a tool.dg section. This is required for pyproject.toml files."
            )
        else:
            yield get_toml_node(toml, ("tool", "dg"), tomlkit.items.Table)


def load_dg_user_file_config(path: Optional[Path] = None) -> DgRawCliConfig:
    path = path or get_dg_config_path()
    contents = load_config(path).get("cli", {})

    return DgRawCliConfig(**{k: v for k, v in contents.items() if k != "plus"})


_OLD_USER_FILE_CONFIG_LOCATION = Path.home() / ".dg.toml"


def has_dg_user_file_config() -> bool:
    # Remove when we remove other deprecated stuff.
    if (Path.home() / ".dg.toml").exists():
        # We can't suppress this warning because we haven't loaded the config with the
        # suppress_warnings list yet.
        emit_warning(
            "deprecated_user_config_location",
            f"""
                Found config file ~/.dg.toml. This location for user config is no longer being read.
                Please move your configuration file to `{get_dg_config_path()}`.
            """,
            None,
            include_suppression_instruction=False,
        )
    return does_dg_config_file_exist()


def load_dg_root_file_config(
    path: Path, config_format: Optional[DgConfigFileFormat] = None
) -> DgFileConfig:
    return _load_dg_file_config(path, config_format)


def load_dg_workspace_file_config(path: Path) -> "DgWorkspaceFileConfig":
    config = _load_dg_file_config(path, None)
    if is_workspace_file_config(config):
        return config
    else:
        _raise_file_config_validation_error("Expected a workspace configuration.", path)


def _load_dg_file_config(path: Path, config_format: Optional[DgConfigFileFormat]) -> DgFileConfig:
    import tomlkit
    import tomlkit.items

    toml = tomlkit.parse(path.read_text())
    config_format = config_format or detect_dg_config_file_format(path)
    if config_format == "root":
        raw_dict = toml.unwrap()
        path_prefix = None
    else:
        raw_dict = get_toml_node(toml, ("tool", "dg"), tomlkit.items.Table).unwrap()
        path_prefix = "tool.dg"
    try:
        config = _DgConfigValidator(path_prefix).validate({k: v for k, v in raw_dict.items()})
    except DgValidationError as e:
        _raise_file_config_validation_error(str(e), path)
    return config


class _DgConfigValidator:
    def __init__(self, path_prefix: Optional[str]) -> None:
        self.path_prefix = path_prefix

    def normalize_deprecated_settings(self, raw_dict: dict[str, Any]) -> None:
        """Normalize deprecated settings to the new format."""
        # We have to separately extract the warning suppression list since we haven't validated the
        # config yet.
        cli_section = raw_dict.get("cli", {})
        self._validate_file_config_setting(
            cli_section, "suppress_warnings", list[DgWarningIdentifier], "cli"
        )
        suppress_warnings = cast(
            "list[DgWarningIdentifier]", cli_section.get("suppress_warnings", [])
        )

        if has_toml_node(raw_dict, ("project", "python_environment")):
            full_key = self._get_full_key("project.python_environment")
            msg = textwrap.dedent(f"""
                Setting `{full_key}` is deprecated. This key can be removed.
            """).strip()
            emit_warning("deprecated_python_environment", msg, suppress_warnings)
            del raw_dict["project"]["python_environment"]

    def validate(self, raw_dict: dict[str, Any]) -> DgFileConfig:
        self.normalize_deprecated_settings(raw_dict)
        self._validate_file_config_setting(
            raw_dict,
            "directory_type",
            Required[Literal["workspace", "project"]],
        )
        self._validate_dg_config_file_cli_section(raw_dict.get("cli", {}))
        if raw_dict["directory_type"] == "workspace":
            self._validate_file_config_setting(raw_dict, "workspace", dict)
            self._validate_file_config_workspace_section(raw_dict.get("workspace", {}))
            self._validate_file_config_no_extraneous_keys(
                set(DgWorkspaceFileConfig.__annotations__.keys()), raw_dict, None
            )
            return cast("DgWorkspaceFileConfig", raw_dict)
        elif raw_dict["directory_type"] == "project":
            self._validate_file_config_setting(raw_dict, "project", dict)
            self._validate_file_config_project_section(raw_dict.get("project", {}))
            self._validate_file_config_no_extraneous_keys(
                set(DgProjectFileConfig.__annotations__.keys()), raw_dict, None
            )
            return cast("DgProjectFileConfig", raw_dict)
        else:
            raise DgError("Unreachable")

    def _validate_dg_config_file_cli_section(self, section: object) -> None:
        if not isinstance(section, dict):
            self._raise_mistyped_key_error("tool.dg.cli", get_type_str(dict), section)
        for key, type_ in DgRawCliConfig.__annotations__.items():
            self._validate_file_config_setting(section, key, type_, "cli")
        self._validate_file_config_no_extraneous_keys(
            set(DgRawCliConfig.__annotations__.keys()), section, "cli"
        )

    def _validate_file_config_project_section(self, section: object) -> None:
        if not isinstance(section, dict):
            self._raise_mistyped_key_error("project", get_type_str(dict), section)
        for key, type_ in DgRawProjectConfig.__annotations__.items():
            self._validate_file_config_setting(section, key, type_, "project")
        self._validate_file_config_no_extraneous_keys(
            set(DgRawProjectConfig.__annotations__.keys()), section, "project"
        )
        if "registry_modules" in section:
            for pattern in section["registry_modules"]:
                if not is_valid_module_pattern(pattern):
                    full_key = self._get_full_key("project.registry_modules")
                    raise DgValidationError(
                        f"Invalid module pattern `{pattern}` at `{full_key}`. "
                        "Module patterns must consist of '.'-separated segments that are either "
                        "valid Python identifiers or wildcards ('*')."
                    )
        if "code_location_target_module" in section and section.get("autoload_defs"):
            autoload_defs_key = self._get_full_key("project.autoload_defs")
            code_location_target_module_key = self._get_full_key(
                "project.code_location_target_module"
            )
            raise DgValidationError(
                f"Cannot specify `{code_location_target_module_key}` when `{autoload_defs_key}` is True. These options are mutually exclusive."
                " Please set `autoload_defs` to False or remove the `code_location_target_module`."
            )

    def _validate_file_config_workspace_section(self, section: object) -> None:
        if not isinstance(section, dict):
            self._raise_mistyped_key_error("workspace", get_type_str(dict), section)
        for key, type_ in DgRawWorkspaceConfig.__annotations__.items():
            if key == "projects":
                self._validate_file_config_setting(section, key, list, "workspace")
                for i, spec in enumerate(section.get("projects") or []):
                    self._validate_file_config_workspace_project_spec(spec, i)
            elif key == "scaffold_project_options":
                self._validate_file_config_workspace_scaffold_project_options(
                    section.get("scaffold_project_options", {})
                )
            else:
                self._validate_file_config_setting(section, key, type_, "workspace")
        self._validate_file_config_no_extraneous_keys(
            set(DgRawWorkspaceConfig.__annotations__.keys()), section, "workspace"
        )

    def _validate_file_config_workspace_project_spec(self, section: object, index: int) -> None:
        if not isinstance(section, dict):
            self._raise_mistyped_key_error(
                f"workspace.projects[{index}]", get_type_str(dict), section
            )
        for key, type_ in DgRawWorkspaceProjectSpec.__annotations__.items():
            self._validate_file_config_setting(section, key, type_, f"workspace.projects[{index}]")
        self._validate_file_config_no_extraneous_keys(
            set(DgRawWorkspaceProjectSpec.__annotations__.keys()),
            section,
            f"workspace.projects[{index}]",
        )

    def _validate_file_config_workspace_scaffold_project_options(self, section: object) -> None:
        if not isinstance(section, dict):
            self._raise_mistyped_key_error(
                "workspace.scaffold_project_options", get_type_str(dict), section
            )
        for key, type_ in DgRawWorkspaceNewProjectOptions.__annotations__.items():
            self._validate_file_config_setting(
                section, key, type_, "workspace.scaffold_project_options"
            )
        self._validate_file_config_no_extraneous_keys(
            set(DgRawWorkspaceNewProjectOptions.__annotations__.keys()),
            section,
            "workspace.scaffold_project_options",
        )

    def _validate_file_config_no_extraneous_keys(
        self, valid_keys: set[str], section: Mapping[str, object], toml_path: Optional[str]
    ) -> None:
        extraneous_keys = [k for k in section.keys() if k not in valid_keys]
        if extraneous_keys:
            full_key = self._get_full_key(toml_path)
            raise DgValidationError(f"Unrecognized fields at `{full_key}`:\n    {extraneous_keys}")

    # expected_type Any to handle typing constructs (`Literal` etc)
    def _validate_file_config_setting(
        self,
        section: Mapping[str, object],
        key: str,
        type_: Any,
        path_prefix: Optional[str] = None,
    ) -> None:
        origin = get_origin(type_)
        is_required = origin is Required
        class_ = type_ if origin not in (Required, NotRequired) else get_args(type_)[0]

        error_type = None
        if is_required and key not in section:
            error_type = "required"
        if key in section and not match_type(section[key], class_):
            error_type = "mistype"
        if error_type:
            full_key = f"{path_prefix}.{key}" if path_prefix else key
            type_str = get_type_str(class_)
            if error_type == "required":
                self._raise_missing_required_key_error(full_key, type_str)
            if error_type == "mistype":
                self._raise_mistyped_key_error(full_key, type_str, section[key])

    def _get_full_key(self, key: Optional[str]) -> str:
        if self.path_prefix:
            return f"{self.path_prefix}.{key}" if key else self.path_prefix
        return key if key else "<root>"

    def _raise_missing_required_key_error(self, key: str, type_str: str) -> Never:
        full_key = self._get_full_key(key)
        raise DgValidationError(
            f"Missing required value for `{full_key}`:\n   Expected {type_str}."
        )

    def _raise_mistyped_key_error(self, key: str, type_str: str, value: object) -> Never:
        full_key = self._get_full_key(key)
        raise DgValidationError(
            f"Invalid value for `{full_key}`:\n    Expected {type_str}, got `{value}`."
        )


def _raise_file_config_validation_error(message: str, file_path: Path) -> Never:
    exit_with_error(
        textwrap.dedent(f"""
        Error in configuration file:
            {file_path}
        """)
        + message,
        do_format=False,
    )


# expected_type Any to handle typing constructs (`Literal` etc)
def get_type_str(t: Any) -> str:
    origin = get_origin(t)
    if origin is None:
        # It's a builtin or normal class
        if hasattr(t, "__name__"):
            return t.__name__  # e.g. 'int', 'bool'
        return str(t)  # Fallback
    else:
        # It's a parametric type like list[str], Union[int, str], etc.
        args = get_args(t)
        arg_strs = sorted([get_type_str(a) for a in args])
        if origin is Union:
            return " | ".join(arg_strs)
        if origin is Literal:
            arg_strs = [f'"{a}"' for a in arg_strs]
            return " | ".join(arg_strs)
        else:
            return f"{_get_origin_name(origin)}[{', '.join(arg_strs)}]"


def _get_origin_name(origin: Any) -> str:
    if origin is Sequence:
        return "Sequence"  # avoid the collections.abc prefix
    else:
        return origin.__name__
