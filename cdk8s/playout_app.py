"""PlayoutInstance — one playout deployment for a single streaming platform.

`PlayoutInstance(platform="youtube", env=...)` emits `playout-youtube` objects
(ConfigMap + Deployment + Service) with an `app: playout-youtube` selector.
playout publishes its stream INTO the per-platform MediaMTX relay (authored
in infra); the Service exposes only the HTTP control surface on :8080
(/vlc/current for tripbot/console reads, health probes, /version) — the
same name tripbot's `VLC_SERVER_HOST` points at after cutover.

Everything is cdk8s.ApiObject with literal specs — the same idiom as the obs
repo, platform-gateway, and tripbot-console.
"""

from __future__ import annotations

import hashlib
import json

import cdk8s
from config import EnvConfig
from constructs import Construct

IMAGE = "ghcr.io/adanalife/playout"
PART_OF = "tripbot"
CONFIG_HASH_ANNOTATION = "adanalife.dev/config-hash"
HTTP_PORT = 8080  # the binary's HTTP_PORT default

# Multi-arch image carrying the `crane` CLI, used by the PreSync image gate to
# probe the registry. gcr.io (not Docker Hub) — the CI base-image-mirror policy
# doesn't apply to a runtime cluster pull.
CRANE_IMAGE = "gcr.io/go-containerregistry/crane:v0.21.7"

# Must match the path baked into the corpus PVs (vlc-server mounts the same
# claims at the same path).
DASHCAM_MOUNT = "/opt/data/Dashcam/_all"

# SM container holding playout's Sentry DSN, JSON shape {"SENTRY_DSN": "…"}.
# Created on the infra side. One Sentry project per component; envs are
# separated by the SENTRY_ENVIRONMENT tag (= ENV in the binary), so the same
# DSN serves stage + prod.
SENTRY_SM_KEY = "/k8s/sentry-playout"
SENTRY_SECRET = "sentry-playout"


def emit_sentry(scope: Construct, env: EnvConfig) -> None:
    """Per-namespace Sentry DSN secret, shared by every playout instance in
    the namespace and envFrom'd by each (emit once per env). ESO extracts the
    SM JSON into a Secret via the env's namespaced SecretStore."""
    _obj(
        scope,
        "sentry",
        api_version="external-secrets.io/v1",
        kind="ExternalSecret",
        name=SENTRY_SECRET,
        namespace=env.namespace,
        labels={"app.kubernetes.io/part-of": PART_OF},
        spec={
            "refreshInterval": "1h",
            "secretStoreRef": {"name": "aws-parameterstore", "kind": "SecretStore"},
            "target": {"name": SENTRY_SECRET, "creationPolicy": "Owner"},
            "dataFrom": [{"extract": {"key": SENTRY_SM_KEY}}],
        },
    )


def _obj(
    scope: Construct,
    id: str,
    *,
    api_version: str,
    kind: str,
    name: str,
    namespace: str,
    labels: dict | None = None,
    annotations: dict | None = None,
    **body,
):
    """ApiObject takes only apiVersion/kind/metadata as props; other top-level
    keys (spec, data, …) land via JsonPatch — the fleet's literal-spec idiom."""
    metadata = {"name": name, "namespace": namespace}
    if labels:
        metadata["labels"] = labels
    if annotations:
        metadata["annotations"] = annotations
    obj = cdk8s.ApiObject(
        scope, id, api_version=api_version, kind=kind, metadata=metadata
    )
    for key, value in body.items():
        obj.add_json_patch(cdk8s.JsonPatch.add(f"/{key}", value))
    return obj


def emit_image_gate(
    scope: Construct,
    *,
    name: str,
    namespace: str,
    labels: dict,
    image_ref: str,
) -> None:
    """Argo PreSync hook asserting `image_ref` exists in the registry before the
    sync reaches the Deployment.

    playout deploys with strategy Recreate (one MediaMTX publisher at a time), so
    a sync to a not-yet-built tag tears the live pod down first and leaves its
    replacement in ImagePullBackOff — a stream outage. PreSync hooks must succeed
    before the main sync wave, so a `crane manifest` that 404s fails the hook,
    aborts the sync, and leaves the running pod untouched. Re-sync once the image
    build lands. Only emitted for pinned (immutable-tag) envs — floating tags
    always resolve to a prior build, so they can't hit this.
    """
    _obj(
        scope,
        "image-gate",
        api_version="batch/v1",
        kind="Job",
        name=f"{name}-image-gate",
        namespace=namespace,
        labels=labels,
        annotations={
            "argocd.argoproj.io/hook": "PreSync",
            # Keep the last gate visible for debugging; replaced on next sync.
            "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation",
        },
        spec={
            "backoffLimit": 2,
            # Cap the wait so a wedged/unschedulable probe fails the sync (pod
            # safe) instead of stalling PreSync forever.
            "activeDeadlineSeconds": 120,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "nodeSelector": {"kubernetes.io/arch": "amd64"},
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [
                        {
                            "name": "image-gate",
                            "image": CRANE_IMAGE,
                            "args": ["manifest", image_ref],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "10m", "memory": "32Mi"},
                                "limits": {"memory": "64Mi"},
                            },
                        }
                    ],
                },
            },
        },
    )


class PlayoutInstance(Construct):
    def __init__(
        self,
        scope: Construct,
        platform: str,  # "twitch" | "youtube"
        *,
        env: EnvConfig,
    ):
        name = f"playout-{platform}"
        super().__init__(scope, name)
        ns = env.namespace

        labels = {
            "app": name,
            "app.kubernetes.io/name": "playout",
            "app.kubernetes.io/instance": name,
            "app.kubernetes.io/part-of": PART_OF,
        }

        # --- ConfigMap ---
        data = {
            "VIDEO_DIR": DASHCAM_MOUNT,
            "OUTPUT": "rtsp",
            "ENCODER": env.encoder,
            # Publish into the per-platform MediaMTX relay (same namespace);
            # OBS reads from MediaMTX, so playout restarts never invalidate
            # the OBS-facing endpoint.
            "RTSP_URL": f"rtsp://mediamtx-{platform}:8554/dashcam",
            # Control plane: NATS commands + lastplayed resume, wire-compatible
            # with vlc-server. NATS runs in the <env>-platform namespace.
            "NATS_URL": f"nats://nats.{env.name}-platform.svc.cluster.local:4222",
            "ENV": env.nats_env,
            "STREAM_PLATFORM": platform,
            # deployment.environment OTLP label = the k8s namespace (prod-1 /
            # stage-1), matching the Go fleet's OTEL_RESOURCE_ATTRIBUTES so
            # playout's series match the shared dashboards' env filter, distinct
            # from ENV, which is the NATS subject env (production / staging).
            "DEPLOYMENT_ENVIRONMENT": env.namespace,
        }
        cm_name = f"{name}-config"
        _obj(
            self,
            "config",
            api_version="v1",
            kind="ConfigMap",
            name=cm_name,
            namespace=ns,
            labels=labels,
            data=data,
        )
        cfg_hash = hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()[:10]

        # --- resources (+ iGPU claim for VAAPI encode) ---
        # The CPU request is the CFS weight under contention — the minipc
        # co-tenants two OBS encoders and the batch pipeline, so prod sizes
        # this for real.
        # Memory: full decode → scale/rate → x264 encode of two concurrent
        # 1080p60 clips (active + prerolled next) — nothing like libvlc's
        # stream-copy vlc-server. 1Gi OOM-killed during preroll on the real
        # corpus. ponytail: 4Gi ceiling is provisional; tune down once steady
        # RSS is measured on stage.
        requests: dict[str, str] = {"cpu": env.cpu_request, "memory": "512Mi"}
        limits: dict[str, str] = {"memory": "4Gi"}
        if env.gpu:
            requests["gpu.intel.com/i915"] = "1"
            limits["gpu.intel.com/i915"] = "1"

        image_ref = f"{IMAGE}:{env.tag_for('playout')}"
        container = {
            "name": "playout",
            "image": image_ref,
            "imagePullPolicy": env.pull_policy_for("playout"),
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
            "envFrom": [
                {"configMapRef": {"name": cm_name}},
                # Sentry DSN. Optional so the pod starts before the
                # ExternalSecret syncs; the binary no-ops without SENTRY_DSN.
                {"secretRef": {"name": SENTRY_SECRET, "optional": True}},
                # Grafana Cloud OTLP endpoint + auth (OTEL_EXPORTER_OTLP_*),
                # the same ESO secret tripbot materializes in this namespace.
                # Optional for the same reason; telemetry gates off without it.
                {"secretRef": {"name": "grafana-cloud-otlp", "optional": True}},
            ],
            "volumeMounts": [
                {
                    "name": "dashcam",
                    "mountPath": DASHCAM_MOUNT,
                    "readOnly": True,
                }
            ],
            "resources": {"requests": requests, "limits": limits},
            "ports": [{"name": "http", "containerPort": HTTP_PORT}],
            "livenessProbe": {
                "httpGet": {"path": "/health/live", "port": "http"},
                "initialDelaySeconds": 5,
                "periodSeconds": 10,
            },
            # Ready = pipeline PLAYING. Known ceiling: rtspclientsink in
            # RECORD mode reports PLAYING without proving data flow, so a
            # wedged-at-preroll pipeline still passes — the RTSP-DESCRIBE
            # watchdog is the real dead-publish detector.
            "readinessProbe": {
                "httpGet": {"path": "/health/ready", "port": "http"},
                "periodSeconds": 10,
            },
        }

        pod_spec: dict = {
            "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
            # Pin to the minipc: the rpi5's four cores are fully spent on OBS
            # compositing, and prod's node-local corpus PVC lives there anyway.
            # (The i915 claim used to enforce this as a side effect.)
            "nodeSelector": {"kubernetes.io/arch": "amd64"},
            "containers": [container],
            "volumes": [
                {
                    "name": "dashcam",
                    "persistentVolumeClaim": {
                        "claimName": env.dashcam_claim,
                        "readOnly": True,
                    },
                }
            ],
        }
        if env.priority_class:
            pod_spec["priorityClassName"] = env.priority_class

        _obj(
            self,
            "deployment",
            api_version="apps/v1",
            kind="Deployment",
            name=name,
            namespace=ns,
            labels=labels,
            spec={
                # Births parked; a console scale-up brings the platform live and
                # Argo ignores .spec.replicas so the scale sticks (infra argocd
                # ignore_replicas). Replica count is runtime-owned, not git-owned.
                "replicas": 0,
                "selector": {"matchLabels": {"app": name}},
                # Recreate: two publishers racing on the same MediaMTX path
                # would fight over it; one owner at a time.
                "strategy": {"type": "Recreate"},
                "template": {
                    "metadata": {
                        "labels": labels,
                        "annotations": {CONFIG_HASH_ANNOTATION: cfg_hash},
                    },
                    "spec": pod_spec,
                },
            },
        )

        # Guard the Recreate teardown against a not-yet-built image (pinned
        # envs only — floating tags always resolve to a prior build).
        if env.is_pinned("playout"):
            emit_image_gate(
                self,
                name=name,
                namespace=ns,
                labels=labels,
                image_ref=image_ref,
            )

        # The control-plane surface tripbot/console dial after cutover
        # (VLC_SERVER_HOST=playout-<platform>). Stream data never transits
        # this Service — that path is playout → MediaMTX over RTSP.
        _obj(
            self,
            "service",
            api_version="v1",
            kind="Service",
            name=name,
            namespace=ns,
            labels=labels,
            spec={
                "selector": {"app": name},
                "ports": [{"name": "http", "port": HTTP_PORT, "targetPort": "http"}],
            },
        )
