"""Render the config file of one local databend-meta node.

config_text = render_meta_config(
    node_id=2,
    ports=MetaNodePorts(admin=28201, grpc=28202, raft=28203),
    raft_dir=Path("node-2/raft"),
    log_dir=Path("node-2/logs"),
    join_addresses=("127.0.0.1:28103",),
)
"""

import json
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

from .local_meta_cluster import MetaNodePorts

# Paths in the config are relative to the node's cwd, which LocalMetaCluster
# sets to the work dir. The security placeholders render empty for a plain
# node.
META_TOML = """\
log_dir                 = "{log_dir}"
admin_api_address       = "127.0.0.1:{admin_port}"
grpc_api_address        = "127.0.0.1:{grpc_port}"
grpc_api_advertise_host = "127.0.0.1"
{grpc_tls}
{grpc_auth}
[raft_config]
id            = {node_id}
raft_dir      = "{raft_dir}"
raft_api_port = {raft_port}
raft_listen_host    = "127.0.0.1"
raft_advertise_host = "127.0.0.1"
{bootstrap_mode}
{raft_security}
"""


def _require_all_or_none(feature: str, settings: tuple) -> None:
    given = [setting for setting in settings if setting is not None]
    if given and len(given) != len(settings):
        raise ValueError(f"{feature} needs all of its settings or none of them")


def _toml_lines(keys: dict) -> str:
    """Render `key = value` lines; json.dumps of a str, bool, int, or list is valid TOML."""
    return "\n".join(f"{key} = {json.dumps(value)}" for key, value in keys.items())


@dataclass(frozen=True)
class MetaGrpcCredential:
    """One accepted gRPC username and password; the password stays out of repr."""

    username: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class MetaSecurityProfile:
    """gRPC auth, gRPC TLS, Raft TLS, and Raft secret settings shared by the nodes."""

    grpc_auth_strict: bool = False
    grpc_credentials: tuple[MetaGrpcCredential, ...] = ()
    grpc_tls_server_cert: Path | None = None
    grpc_tls_server_key: Path | None = None
    raft_tls_server_cert: Path | None = None
    raft_tls_server_key: Path | None = None
    raft_tls_client_root_ca_cert: Path | None = None
    raft_tls_client_domain_name: str | None = None
    raft_secret: str | None = field(default=None, repr=False)
    raft_accepted_secrets: tuple[str, ...] = field(default=(), repr=False)
    raft_secret_strict: bool = False

    def __post_init__(self):
        grpc_tls = (self.grpc_tls_server_cert, self.grpc_tls_server_key)
        _require_all_or_none("gRPC TLS", grpc_tls)

        raft_tls = (
            self.raft_tls_server_cert,
            self.raft_tls_server_key,
            self.raft_tls_client_root_ca_cert,
            self.raft_tls_client_domain_name,
        )
        _require_all_or_none("Raft TLS", raft_tls)

    def grpc_tls_toml(self) -> str:
        """Top-level keys that enable TLS on the gRPC listener."""
        if self.grpc_tls_server_cert is None:
            return ""
        return _toml_lines(
            {
                "grpc_tls_server_cert": str(self.grpc_tls_server_cert),
                "grpc_tls_server_key": str(self.grpc_tls_server_key),
            }
        )

    def grpc_auth_toml(self) -> str:
        """The `[grpc_auth]` table with one `[[grpc_auth.credentials]]` per credential."""
        if not self.grpc_auth_strict and not self.grpc_credentials:
            return ""
        lines = ["[grpc_auth]", f"strict = {json.dumps(self.grpc_auth_strict)}"]
        for credential in self.grpc_credentials:
            lines.append("[[grpc_auth.credentials]]")
            lines.append(f"username = {json.dumps(credential.username)}")
            lines.append(f"password = {json.dumps(credential.password)}")
        return "\n".join(lines)

    def raft_security_toml(self, raft_tls_port: int | None) -> str:
        """`[raft_config]` keys for the Raft secret and, when set, Raft TLS."""
        keys = {}
        if self.raft_secret is not None:
            keys["raft_secret"] = self.raft_secret
        if self.raft_accepted_secrets:
            keys["raft_accepted_secrets"] = list(self.raft_accepted_secrets)
        if self.raft_secret_strict:
            keys["raft_secret_strict"] = True
        if self.raft_tls_server_cert is not None:
            keys["raft_tls_server_cert"] = str(self.raft_tls_server_cert)
            keys["raft_tls_server_key"] = str(self.raft_tls_server_key)
            keys["raft_tls_client_root_ca_cert"] = str(
                self.raft_tls_client_root_ca_cert
            )
            keys["raft_tls_client_domain_name"] = self.raft_tls_client_domain_name
            keys["raft_tls_port"] = raft_tls_port
        return _toml_lines(keys)


def write_password_file(path: Path, password: str) -> Path:
    """Write `password` and a newline to `path`, creating its directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(password + "\n")
    return path


@dataclass(frozen=True)
class MetaClientProfile:
    """What a client presents to a Meta gRPC endpoint: credentials and TLS trust.

    `cli_args()` renders the global options shared by metactl, metabench, and
    metaverifier. Nothing is validated here: a test builds a wrong or
    incomplete profile on purpose, with dataclasses.replace(), to check how
    the client rejects it.
    """

    username: str | None = None
    password_file: Path | None = None
    grpc_tls_ca_cert: Path | None = None
    grpc_tls_domain_name: str | None = None

    @classmethod
    def for_credential(
        cls, credential: MetaGrpcCredential, secrets_dir: Path
    ) -> "MetaClientProfile":
        """A plaintext profile for `credential`; its password goes through a file."""
        password_path = secrets_dir / f"{credential.username}.password"
        password_file = write_password_file(password_path, credential.password)
        return cls(credential.username, password_file)

    def cli_args(self) -> list[str]:
        """The global command-line options that carry this profile."""
        args = []
        if self.username is not None:
            args += ["--user", self.username]
        if self.password_file is not None:
            args += ["--password-file", str(self.password_file)]
        if self.grpc_tls_ca_cert is not None:
            args += ["--grpc-tls-ca-cert", str(self.grpc_tls_ca_cert)]
        if self.grpc_tls_domain_name is not None:
            args += ["--grpc-tls-domain-name", self.grpc_tls_domain_name]
        return args


def render_meta_config(
    node_id: int,
    ports: MetaNodePorts,
    *,
    raft_dir: Path,
    log_dir: Path,
    security: MetaSecurityProfile = MetaSecurityProfile(),
    join_addresses: tuple[str, ...] = (),
) -> str:
    """Config text of one node; it bootstraps alone unless `join_addresses` is given."""
    raft_tls_configured = security.raft_tls_server_cert is not None
    if raft_tls_configured != (ports.raft_tls is not None):
        raise ValueError("Raft TLS settings and ports.raft_tls must be given together")

    if join_addresses:
        bootstrap_mode = f"join = {json.dumps(list(join_addresses))}"
    else:
        bootstrap_mode = "single = true"

    return META_TOML.format(
        log_dir=log_dir,
        admin_port=ports.admin,
        grpc_port=ports.grpc,
        grpc_tls=security.grpc_tls_toml(),
        grpc_auth=security.grpc_auth_toml(),
        node_id=node_id,
        raft_dir=raft_dir,
        raft_port=ports.raft,
        bootstrap_mode=bootstrap_mode,
        raft_security=security.raft_security_toml(ports.raft_tls),
    )
