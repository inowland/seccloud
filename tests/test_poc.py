from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import seccloud.api as seccloud_api
import seccloud.workers as seccloud_workers
from seccloud.cli import (
    bootstrap_local_runtime,
    build_operator_runtime_status,
    build_parser,
    run_demo_stream,
)
from seccloud.detection_context import build_detection_context, ensure_detection_context
from seccloud.feature_lake import load_feature_lake_feature_set, load_feature_lake_snapshot
from seccloud.investigation import (
    acknowledge_detection,
    build_evidence_bundle,
    build_peer_comparison,
    create_case_from_detection,
    get_detection_detail,
    get_entity_timeline,
    get_event_detail,
    list_detections,
    summarize_case,
    update_case,
)
from seccloud.local_postgres import _cleanup_stale_runtime_files, _wait_for_socket_ready, postgres_paths
from seccloud.ml_scoring import _load_feature_set_for_scoring
from seccloud.model_artifact import (
    activate_model_artifact,
    build_model_runtime_status,
    deactivate_model_artifact,
    install_model_artifact_bundle,
    list_installed_model_artifacts,
    load_model_promotion_policy,
    rollback_model_artifact,
    save_model_promotion_policy,
)
from seccloud.model_training import export_workspace_model_artifact
from seccloud.onboarding import (
    build_onboarding_report_markdown,
    build_source_manifest,
    import_fixture_bundle,
    validate_fixture_bundle,
)
from seccloud.pipeline import (
    collect_ops_metadata,
    ingest_raw_events,
    rebuild_derived_state,
    run_runtime,
    seed_workspace,
)
from seccloud.reports import (
    build_conversation_pack_markdown,
    export_founder_artifacts,
)
from seccloud.runtime_stream import (
    advance_runtime_stream,
    get_runtime_stream_state,
    initialize_runtime_stream,
)
from seccloud.source_pack import (
    build_source_capability_markdown,
    build_source_capability_matrix,
)
from seccloud.storage import Workspace
from seccloud.synthetic_scale import OrgPrincipal, OrgTeam
from seccloud.vendor_exports import (
    build_vendor_mapping_report_markdown,
    build_vendor_source_manifest,
    import_vendor_fixture_bundle,
    validate_vendor_fixture_bundle,
)
from seccloud.workers import (
    get_worker_state,
    run_all_local_workers,
    run_detection_mode_comparison,
    run_detection_mode_evaluation,
    run_detection_threshold_sweep,
    run_detection_worker,
    run_feature_builder,
    run_local_processing_workers,
    run_normalization_worker,
    run_projector_worker,
    run_source_stats_projector,
    run_worker_service_loop,
    run_worker_service_once,
    submit_grouped_raw_events,
)


class PoCTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = Path(tempfile.mkdtemp(prefix="seccloud-poc-"))
        self.workspace = Workspace(self.tempdir)
        self.workspace.bootstrap()

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _write_installed_model_bundle(self, model_id: str, *, source: str = "okta") -> Path:
        model_dir = self.workspace.models_dir / model_id
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / f"action_tower_{source}.onnx").write_bytes(b"onnx")
        (model_dir / "context_tower.onnx").write_bytes(b"onnx")
        (model_dir / "principal-vocab.json").write_text(json.dumps(["principal-1"]), encoding="utf-8")
        (model_dir / "resource-vocab.json").write_text(json.dumps(["resource-1"]), encoding="utf-8")
        (model_dir / "categorical-vocabs.json").write_text(
            json.dumps({"role": {"analyst": 0}}),
            encoding="utf-8",
        )
        (model_dir / "eval-report.json").write_text(
            json.dumps(
                {
                    "status": "trained_local_workspace",
                    "model_id": model_id,
                    "training_pair_count": 8,
                    "losses": [0.5, 0.25],
                    "final_loss": 0.25,
                    "activation_gate": {
                        "eligible": True,
                        "reason": "trained_local_workspace_eval_pass",
                        "final_loss": 0.25,
                        "evaluated_source_count": 2,
                        "failing_sources": [],
                        "source_gates": {
                            "okta": {
                                "eligible": True,
                                "reason": "source_sampled_retrieval_gate_pass",
                                "evaluation_scope": "heldout:okta",
                                "sampled_top1_accuracy": 0.9,
                                "pairwise_win_rate": 0.9,
                                "mean_margin": 0.2,
                                "pair_count": 6,
                            },
                            "github": {
                                "eligible": True,
                                "reason": "source_sampled_retrieval_gate_pass",
                                "evaluation_scope": "heldout:github",
                                "sampled_top1_accuracy": 0.85,
                                "pairwise_win_rate": 0.87,
                                "mean_margin": 0.18,
                                "pair_count": 6,
                            },
                        },
                        "coverage_gate": {
                            "eligible": True,
                            "reason": "cross_domain_coverage_gate_pass",
                            "covered_sources": ["github", "okta"],
                            "covered_identity_sources": ["okta"],
                            "covered_resource_sources": ["github"],
                            "required_source_count": 2,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        (model_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "artifact_version": 1,
                    "tenant_id": self.workspace.tenant_id,
                    "model_id": model_id,
                    "model_version": model_id,
                    "model_family": "contrastive-facade",
                    "scoring_mode": "onnx",
                    "exported_at": "2026-03-17T00:00:00Z",
                    "feature_schema_version": "feature.v1",
                    "required_feature_tables": ["action", "history", "static", "peer_group"],
                    "padding": {"max_tokens": 8},
                    "action_towers": {
                        source: {
                            "path": f"action_tower_{source}.onnx",
                            "sha256": "sha256:test",
                            "input_names": ["indices", "weights", "mask"],
                            "output_name": "embedding",
                        }
                    },
                    "context_tower": {
                        "path": "context_tower.onnx",
                        "sha256": "sha256:test",
                        "input_names": ["hist_window_indices"],
                        "output_name": "embedding",
                    },
                    "eval_report_path": "eval-report.json",
                    "score_policy": {
                        "detection_threshold": 0.6,
                        "high_severity_threshold": 0.8,
                        "calibration_source": "default_fallback",
                        "calibration_reason": "legacy_default_thresholds",
                        "source_policies": {
                            "okta": {
                                "detection_threshold": 0.55,
                                "high_severity_threshold": 0.75,
                                "calibration_source": "heldout:okta",
                                "calibration_reason": "test_source_policy",
                                "evaluation_pair_count": 4,
                            },
                            "github": {
                                "detection_threshold": 0.5,
                                "high_severity_threshold": 0.7,
                                "calibration_source": "heldout:github",
                                "calibration_reason": "test_source_policy",
                                "evaluation_pair_count": 4,
                            },
                        },
                    },
                    "input_vocabs": {
                        "principal_entity_keys_path": "principal-vocab.json",
                        "resource_entity_keys_path": "resource-vocab.json",
                        "categorical_vocabs_path": "categorical-vocabs.json",
                        "principal_vocab_count": 1,
                        "resource_vocab_count": 1,
                        "categorical_vocab_counts": {"role": 1},
                    },
                }
            ),
            encoding="utf-8",
        )
        return model_dir

    def test_seed_and_normalize_all_sources(self) -> None:
        seed_result = seed_workspace(self.workspace)
        self.assertGreater(seed_result["raw_event_count"], 0)

        ingest_result = ingest_raw_events(self.workspace)
        normalized = self.workspace.list_normalized_events()
        sources = {event["source"] for event in normalized}

        self.assertEqual(sources, {"okta", "gworkspace", "github", "snowflake"})
        self.assertEqual(ingest_result["normalized_events_seen"], len(normalized))
        self.assertEqual(ingest_result["dead_letter_count"], 2)
        self.assertEqual(ingest_result["duplicate_semantic_events"], 1)
        self.assertEqual(ingest_result["late_arrival_count"], 1)
        for event in normalized:
            self.assertIn("event_id", event)
            self.assertIn("evidence", event)
            self.assertIn("principal", event)
            self.assertIn("resource", event)
            self.assertIn("action", event)
        self.assertEqual(len(self.workspace.list_dead_letters()), 2)

    def test_replay_is_idempotent(self) -> None:
        seed_workspace(self.workspace)
        first = ingest_raw_events(self.workspace)
        second = ingest_raw_events(self.workspace)

        self.assertGreater(first["added_normalized_events"], 0)
        self.assertEqual(second["added_normalized_events"], 0)
        self.assertEqual(second["dead_letter_count"], 0)
        self.assertEqual(second["duplicate_semantic_events"], 53)
        self.assertEqual(len(self.workspace.list_normalized_events()), first["normalized_events_seen"])

    def test_normalized_events_fall_back_to_lake_when_json_segments_are_missing(self) -> None:
        seed_workspace(self.workspace)
        ingest_raw_events(self.workspace)
        self.assertEqual(list(self.workspace.normalized_dir.rglob("*.json")), [])
        shutil.rmtree(self.workspace.normalized_dir)

        normalized = self.workspace.list_normalized_events()

        self.assertGreater(len(normalized), 0)
        self.assertEqual({event["source"] for event in normalized}, {"okta", "gworkspace", "github", "snowflake"})

    def test_detections_fall_back_to_lake_and_ack_overlay(self) -> None:
        run_runtime(self.workspace)
        original = self.workspace.list_detections()
        self.assertGreater(len(original), 0)
        shutil.rmtree(self.workspace.detections_dir)
        self.workspace.detections_dir.mkdir(parents=True, exist_ok=True)

        from_lake = self.workspace.list_detections()

        self.assertEqual(
            {item["detection_id"] for item in from_lake},
            {item["detection_id"] for item in original},
        )

        updated = acknowledge_detection(self.workspace, from_lake[0]["detection_id"])

        self.assertEqual(updated["status"], "acknowledged")
        self.assertEqual(
            self.workspace.get_detection(from_lake[0]["detection_id"])["status"],
            "acknowledged",
        )

    def test_retention_and_rebuild_preserve_derived_state(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        case = create_case_from_detection(self.workspace, detections[0]["detection_id"])
        before = self.workspace.load_derived_state()

        retention = self.workspace.apply_raw_retention(7, "2026-01-10T23:59:59Z")
        after_raw = self.workspace.list_raw_events()
        rebuilt = rebuild_derived_state(self.workspace)
        after = self.workspace.load_derived_state()

        self.assertGreater(len(retention["deleted_raw_objects"]), 0)
        self.assertLess(len(after_raw), 53)
        self.assertIn("principal_profiles", after)
        self.assertEqual(after["principal_profiles"], {})
        self.assertEqual(after["peer_groups"], {})
        self.assertEqual(after["access_histories"], {})
        self.assertEqual(after["embeddings"], {})
        self.assertEqual(before["case_artifacts"], after["case_artifacts"])
        self.assertEqual(before["metadata"]["normalized_event_count"], after["metadata"]["normalized_event_count"])
        self.assertEqual(rebuilt["normalized_event_count"], after["metadata"]["normalized_event_count"])
        reloaded_case = self.workspace.get_case(case["case_id"])
        self.assertTrue(reloaded_case["evidence_snapshots"])

    def test_rebuild_derived_state_uses_event_index(self) -> None:
        run_runtime(self.workspace)

        with patch.object(
            self.workspace,
            "list_normalized_events",
            side_effect=AssertionError("should not scan normalized events"),
        ):
            rebuilt = rebuild_derived_state(self.workspace)

        self.assertGreater(rebuilt["normalized_event_count"], 0)

    def test_runtime_persists_detection_context_manifest(self) -> None:
        run_runtime(self.workspace)

        detection_context = self.workspace.load_detection_context()

        self.assertEqual(
            detection_context["event_count"],
            self.workspace.ensure_event_index()["event_count"],
        )
        self.assertTrue(detection_context["ordered_event_ids"])
        self.assertIn("events_by_source", detection_context["aggregates"])
        first_context = next(iter(detection_context["contexts_by_event_id"].values()))
        self.assertIn("geo_history_count", first_context)
        self.assertIn("geo_seen_before", first_context)

    def test_model_runtime_status_reads_active_model_artifact(self) -> None:
        self._write_installed_model_bundle("contrastive-demo-v1")
        self.workspace.save_model_artifact_manifest(
            {
                "manifest_version": 2,
                "requested_mode": "onnx",
                "active_model_id": "contrastive-demo-v1",
                "active_metadata_path": "models/contrastive-demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "test",
                "activation_history": [],
            }
        )

        runtime = build_model_runtime_status(self.workspace)
        status = build_operator_runtime_status(self.workspace, dsn=None, runtime_root=self.tempdir)

        self.assertTrue(runtime["available"])
        self.assertEqual(runtime["requested_mode"], "onnx")
        self.assertEqual(runtime["effective_mode"], "onnx_native")
        self.assertEqual(runtime["reason"], "model_artifact_loaded_onnx_native")
        self.assertEqual(runtime["model_version"], "contrastive-demo-v1")
        self.assertEqual(runtime["principal_vocab_count"], 1)
        self.assertEqual(runtime["resource_vocab_count"], 1)
        self.assertTrue(runtime["activation_gate"]["eligible"])
        self.assertEqual(
            runtime["activation_gate"]["reason"],
            "trained_local_workspace_eval_pass",
        )
        self.assertEqual(runtime["installed_model_count"], 1)
        self.assertEqual(runtime["installed_model_ids"], ["contrastive-demo-v1"])
        self.assertEqual(runtime["score_policy"]["detection_threshold"], 0.6)
        self.assertEqual(runtime["score_policy"]["high_severity_threshold"], 0.8)
        self.assertEqual(runtime["score_policy"]["source_policies"]["okta"]["detection_threshold"], 0.55)
        self.assertEqual(runtime["activation_gate"]["evaluated_source_count"], 2)
        self.assertEqual(
            runtime["activation_gate"]["source_gates"]["okta"]["reason"],
            "source_sampled_retrieval_gate_pass",
        )
        self.assertTrue(runtime["activation_gate"]["coverage_gate"]["eligible"])
        self.assertEqual(runtime["promotion_policy"]["required_source_count"], 2)
        self.assertTrue(runtime["promotion_policy"]["require_identity_coverage"])
        self.assertTrue(runtime["promotion_policy"]["require_resource_coverage"])
        self.assertEqual(status["model_runtime"]["model_id"], "contrastive-demo-v1")

    def test_model_artifact_lifecycle_supports_activate_deactivate_and_rollback(self) -> None:
        self._write_installed_model_bundle("demo-v1")
        self._write_installed_model_bundle("demo-v2", source="github")

        listed = list_installed_model_artifacts(self.workspace)
        self.assertEqual([item["model_id"] for item in listed], ["demo-v1", "demo-v2"])

        activated = activate_model_artifact(self.workspace, "demo-v1")
        active_runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(activated["requested_mode"], "onnx")
        self.assertEqual(active_runtime["model_id"], "demo-v1")
        self.assertEqual(active_runtime["effective_mode"], "onnx_native")

        activate_model_artifact(self.workspace, "demo-v2")
        switched_runtime = build_model_runtime_status(self.workspace)
        self.assertEqual(switched_runtime["model_id"], "demo-v2")

        deactivated = deactivate_model_artifact(self.workspace)
        heuristic_runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(deactivated["requested_mode"], "heuristic")
        self.assertEqual(heuristic_runtime["effective_mode"], "heuristic")
        self.assertEqual(heuristic_runtime["reason"], "requested_heuristic_mode")
        self.assertEqual(heuristic_runtime["installed_model_count"], 2)

        rolled_back = rollback_model_artifact(self.workspace)
        rolled_back_runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(rolled_back["active_model_id"], "demo-v2")
        self.assertEqual(rolled_back_runtime["model_id"], "demo-v2")
        self.assertEqual(rolled_back_runtime["effective_mode"], "onnx_native")
        self.assertGreaterEqual(len(rolled_back["activation_history"]), 4)

    def test_install_model_artifact_bundle_supports_in_place_workspace_model_dir(self) -> None:
        model_dir = self._write_installed_model_bundle("local-m3-demo")

        manifest = install_model_artifact_bundle(
            self.workspace,
            model_dir,
            activation_source="workspace_train_export",
        )
        runtime = build_model_runtime_status(self.workspace)

        self.assertTrue((model_dir / "metadata.json").exists())
        self.assertEqual(manifest["active_model_id"], "local-m3-demo")
        self.assertEqual(runtime["model_id"], "local-m3-demo")
        self.assertEqual(runtime["effective_mode"], "onnx_native")

    def test_model_activation_gate_blocks_promotion_without_force(self) -> None:
        model_dir = self._write_installed_model_bundle("blocked-v1")
        (model_dir / "eval-report.json").write_text(
            json.dumps(
                {
                    "status": "trained_local_workspace",
                    "model_id": "blocked-v1",
                    "training_pair_count": 8,
                    "losses": [0.9],
                    "final_loss": 0.9,
                    "activation_gate": {
                        "eligible": False,
                        "reason": "eval_metric_below_threshold",
                        "final_loss": 0.9,
                    },
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "model artifact failed activation gate"):
            activate_model_artifact(self.workspace, "blocked-v1")

        forced = activate_model_artifact(self.workspace, "blocked-v1", force=True)
        runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(forced["active_model_id"], "blocked-v1")
        self.assertFalse(runtime["activation_gate"]["eligible"])
        self.assertEqual(runtime["activation_gate"]["reason"], "eval_metric_below_threshold")

    def test_model_activation_gate_blocks_promotion_when_source_gate_fails(self) -> None:
        model_dir = self._write_installed_model_bundle("blocked-source-v1", source="github")
        (model_dir / "eval-report.json").write_text(
            json.dumps(
                {
                    "status": "trained_local_workspace",
                    "model_id": "blocked-source-v1",
                    "training_pair_count": 12,
                    "losses": [0.4],
                    "final_loss": 0.4,
                    "activation_gate": {
                        "eligible": False,
                        "reason": "source_gate_failed:github",
                        "final_loss": 0.4,
                        "evaluation_scope": "heldout",
                        "sampled_top1_accuracy": 0.8,
                        "pairwise_win_rate": 0.8,
                        "mean_margin": 0.1,
                        "evaluated_source_count": 2,
                        "failing_sources": ["github"],
                        "source_gates": {
                            "github": {
                                "eligible": False,
                                "reason": "pairwise_win_rate_below_threshold",
                                "evaluation_scope": "heldout:github",
                                "sampled_top1_accuracy": 0.4,
                                "pairwise_win_rate": 0.4,
                                "mean_margin": 0.01,
                                "pair_count": 6,
                            },
                            "okta": {
                                "eligible": True,
                                "reason": "source_sampled_retrieval_gate_pass",
                                "evaluation_scope": "heldout:okta",
                                "sampled_top1_accuracy": 0.9,
                                "pairwise_win_rate": 0.9,
                                "mean_margin": 0.2,
                                "pair_count": 6,
                            },
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "source_gate_failed:github"):
            activate_model_artifact(self.workspace, "blocked-source-v1")

        forced = activate_model_artifact(self.workspace, "blocked-source-v1", force=True)
        runtime_after = build_model_runtime_status(self.workspace)

        self.assertEqual(forced["active_model_id"], "blocked-source-v1")
        self.assertEqual(runtime_after["activation_gate"]["reason"], "source_gate_failed:github")
        self.assertEqual(runtime_after["activation_gate"]["failing_sources"], ["github"])

    def test_model_activation_gate_blocks_promotion_when_cross_domain_coverage_is_missing(self) -> None:
        model_dir = self._write_installed_model_bundle("coverage-blocked-v1", source="okta")
        (model_dir / "eval-report.json").write_text(
            json.dumps(
                {
                    "status": "trained_local_workspace",
                    "model_id": "coverage-blocked-v1",
                    "training_pair_count": 12,
                    "losses": [0.3],
                    "final_loss": 0.3,
                    "activation_gate": {
                        "eligible": False,
                        "reason": "coverage_gate_failed:resource_source_coverage_missing",
                        "final_loss": 0.3,
                        "evaluation_scope": "heldout",
                        "sampled_top1_accuracy": 0.8,
                        "pairwise_win_rate": 0.8,
                        "mean_margin": 0.1,
                        "evaluated_source_count": 1,
                        "failing_sources": [],
                        "source_gates": {
                            "okta": {
                                "eligible": True,
                                "reason": "source_sampled_retrieval_gate_pass",
                                "evaluation_scope": "heldout:okta",
                                "sampled_top1_accuracy": 0.9,
                                "pairwise_win_rate": 0.9,
                                "mean_margin": 0.2,
                                "pair_count": 6,
                            }
                        },
                        "coverage_gate": {
                            "eligible": False,
                            "reason": "resource_source_coverage_missing",
                            "covered_sources": ["okta"],
                            "covered_identity_sources": ["okta"],
                            "covered_resource_sources": [],
                            "required_source_count": 2,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            ValueError,
            "coverage_gate_failed:insufficient_covered_sources",
        ):
            activate_model_artifact(self.workspace, "coverage-blocked-v1")

        forced = activate_model_artifact(self.workspace, "coverage-blocked-v1", force=True)
        runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(forced["active_model_id"], "coverage-blocked-v1")
        self.assertFalse(runtime["activation_gate"]["coverage_gate"]["eligible"])
        self.assertEqual(
            runtime["activation_gate"]["coverage_gate"]["reason"],
            "insufficient_covered_sources",
        )

    def test_model_activation_gate_respects_workspace_promotion_policy(self) -> None:
        self._write_installed_model_bundle("coverage-relaxed-v1", source="okta")
        policy = save_model_promotion_policy(
            self.workspace,
            required_source_count=1,
            require_identity_coverage=True,
            require_resource_coverage=False,
            identity_sources=["okta"],
            resource_sources=["github", "gworkspace", "snowflake"],
        )

        activated = activate_model_artifact(self.workspace, "coverage-relaxed-v1")
        runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(activated["active_model_id"], "coverage-relaxed-v1")
        self.assertTrue(runtime["activation_gate"]["eligible"])
        self.assertTrue(runtime["activation_gate"]["coverage_gate"]["eligible"])
        self.assertEqual(runtime["promotion_policy"], policy)

    def test_model_activation_gate_uses_current_policy_when_reinterpreting_legacy_eval_report(self) -> None:
        model_dir = self._write_installed_model_bundle("coverage-legacy-v1", source="okta")
        (model_dir / "eval-report.json").write_text(
            json.dumps(
                {
                    "status": "trained_local_workspace",
                    "model_id": "coverage-legacy-v1",
                    "training_pair_count": 12,
                    "losses": [0.3],
                    "final_loss": 0.3,
                    "activation_gate": {
                        "eligible": False,
                        "reason": "coverage_gate_failed:resource_source_coverage_missing",
                        "final_loss": 0.3,
                        "evaluation_scope": "heldout",
                        "sampled_top1_accuracy": 0.8,
                        "pairwise_win_rate": 0.8,
                        "mean_margin": 0.1,
                        "evaluated_source_count": 1,
                        "failing_sources": [],
                        "source_gates": {
                            "okta": {
                                "eligible": True,
                                "reason": "source_sampled_retrieval_gate_pass",
                                "evaluation_scope": "heldout:okta",
                                "sampled_top1_accuracy": 0.9,
                                "pairwise_win_rate": 0.9,
                                "mean_margin": 0.2,
                                "pair_count": 6,
                            }
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        save_model_promotion_policy(
            self.workspace,
            required_source_count=1,
            require_identity_coverage=True,
            require_resource_coverage=False,
            identity_sources=["okta"],
            resource_sources=["github", "gworkspace", "snowflake"],
        )

        activated = activate_model_artifact(self.workspace, "coverage-legacy-v1")
        runtime = build_model_runtime_status(self.workspace)

        self.assertEqual(activated["active_model_id"], "coverage-legacy-v1")
        self.assertTrue(runtime["activation_gate"]["eligible"])
        self.assertEqual(
            runtime["activation_gate"]["reason"],
            "activation_gate_declared",
        )
        self.assertEqual(
            runtime["activation_gate"]["coverage_gate"]["reason"],
            "cross_domain_coverage_gate_pass",
        )

    def test_model_promotion_policy_cli_round_trip(self) -> None:
        parser = build_parser()
        show_args = parser.parse_args(["show-model-promotion-policy", "--workspace", str(self.workspace.root)])
        self.assertEqual(show_args.command, "show-model-promotion-policy")

        args = parser.parse_args(
            [
                "set-model-promotion-policy",
                "--workspace",
                str(self.workspace.root),
                "--required-source-count",
                "3",
                "--no-require-identity-coverage",
                "--resource-source",
                "github",
                "--resource-source",
                "snowflake",
            ]
        )
        self.assertEqual(args.command, "set-model-promotion-policy")
        save_model_promotion_policy(
            self.workspace,
            required_source_count=args.required_source_count,
            require_identity_coverage=args.require_identity_coverage,
            require_resource_coverage=(
                True if args.require_resource_coverage is None else args.require_resource_coverage
            ),
            identity_sources=load_model_promotion_policy(self.workspace)["identity_sources"],
            resource_sources=args.resource_source,
        )

        policy = load_model_promotion_policy(self.workspace)
        self.assertEqual(policy["required_source_count"], 3)
        self.assertFalse(policy["require_identity_coverage"])
        self.assertEqual(policy["resource_sources"], ["github", "snowflake"])

    def test_rust_detection_worker_runs_native_ort_model(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        import torch
        import torch.nn as nn

        class ActionTower(nn.Module):
            def forward(self, indices, weights, mask):
                weighted = weights.sum(dim=1, keepdim=True)
                keep_inputs = indices.to(torch.float32).sum(dim=1, keepdim=True) * 0
                keep_inputs = keep_inputs + mask.to(torch.float32).sum(dim=1, keepdim=True) * 0
                return torch.cat([weighted + keep_inputs, torch.zeros_like(weighted)], dim=1)

        class ContextTower(nn.Module):
            def forward(
                self,
                hist_window_indices,
                hist_window_mask,
                hist_num_windows,
                peer_indices,
                peer_weights,
                peer_mask,
                role,
                location,
                duration,
                privilege,
            ):
                batch = role.shape[0]
                dummy = torch.zeros((batch, 1), dtype=torch.float32)
                dummy = dummy + hist_window_indices.to(torch.float32).sum(dim=(1, 2), keepdim=False).unsqueeze(1) * 0
                dummy = dummy + hist_window_mask.to(torch.float32).sum(dim=(1, 2), keepdim=False).unsqueeze(1) * 0
                dummy = dummy + hist_num_windows.to(torch.float32).unsqueeze(1) * 0
                dummy = dummy + peer_indices.to(torch.float32).sum(dim=(1, 2), keepdim=False).unsqueeze(1) * 0
                dummy = dummy + peer_weights.sum(dim=(1, 2), keepdim=False).unsqueeze(1) * 0
                dummy = dummy + peer_mask.to(torch.float32).sum(dim=(1, 2), keepdim=False).unsqueeze(1) * 0
                dummy = dummy + role.to(torch.float32).unsqueeze(1) * 0
                dummy = dummy + location.to(torch.float32).unsqueeze(1) * 0
                dummy = dummy + duration.to(torch.float32).unsqueeze(1) * 0
                dummy = dummy + privilege.to(torch.float32).unsqueeze(1) * 0
                return torch.cat([dummy, dummy + 1], dim=1)

        def write_json(path: Path, payload: dict[str, object]) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), path)

        event = {
            "event_id": "evt_native",
            "event_key": "evk_evt_native",
            "source": "github",
            "observed_at": "2026-03-10T10:00:00Z",
            "source_event_id": "src_evt_native",
            "principal": {
                "id": "alice@example.com",
                "entity_id": "ent_alice",
                "entity_key": "enk_alice@example.com",
                "department": "engineering",
                "role": "staff-engineer",
            },
            "resource": {
                "id": "repo-secret",
                "entity_id": "ent_repo-secret",
                "entity_key": "enk_repo-secret",
                "sensitivity": "critical",
            },
            "action": {
                "source": "github",
                "verb": "clone",
                "category": "code_access",
            },
            "attributes": {},
            "evidence": {
                "source": "github",
                "object_key": "raw/object.json",
                "raw_event_id": "raw-1",
                "observed_at": "2026-03-10T10:00:00Z",
            },
        }
        write_json(self.workspace.normalized_dir / "source=github" / "evt_native.json", event)
        write_json(
            self.workspace.source_stats_path,
            {
                "sources": {
                    "github": {
                        "normalized_event_count": 1,
                        "dead_letter_count": 0,
                        "last_seen_at": "2026-03-10T10:00:00Z",
                    }
                }
            },
        )

        action_manifest = "lake/manifests/action.json"
        history_manifest = "lake/manifests/history.json"
        static_manifest = "lake/manifests/static.json"
        write_parquet(
            self.workspace.root / "lake" / "features" / "action.parquet",
            [
                {
                    "feature_schema_version": "feature.v1",
                    "tenant_id": self.workspace.tenant_id,
                    "source": "github",
                    "resource_entity_key": "enk_repo-secret",
                    "principal_entity_key": "enk_alice@example.com",
                    "access_count": 1,
                    "accessor_weight": 1.0,
                }
            ],
        )
        write_json(self.workspace.root / action_manifest, {"objects": [{"object_key": "lake/features/action.parquet"}]})
        write_parquet(
            self.workspace.root / "lake" / "features" / "history.parquet",
            [
                {
                    "feature_schema_version": "feature.v1",
                    "tenant_id": self.workspace.tenant_id,
                    "principal_entity_key": "enk_alice@example.com",
                    "window_start": "2026-03-10T10:00:00Z",
                    "resource_entity_key": "enk_repo-secret",
                }
            ],
        )
        write_json(
            self.workspace.root / history_manifest,
            {"objects": [{"object_key": "lake/features/history.parquet"}]},
        )
        write_parquet(
            self.workspace.root / "lake" / "features" / "static.parquet",
            [
                {
                    "feature_schema_version": "feature.v1",
                    "tenant_id": self.workspace.tenant_id,
                    "principal_entity_key": "enk_alice@example.com",
                    "principal_id": "alice@example.com",
                    "department": "engineering",
                    "role": "staff-engineer",
                    "location": "unknown",
                    "employment_duration_bucket": "1-3yr",
                    "privilege_level": "elevated",
                }
            ],
        )
        write_json(self.workspace.root / static_manifest, {"objects": [{"object_key": "lake/features/static.parquet"}]})
        write_json(
            self.workspace.feature_state_path,
            {
                "action_manifest_key": action_manifest,
                "history_manifest_key": history_manifest,
                "static_manifest_key": static_manifest,
            },
        )

        model_dir = self.workspace.models_dir / "demo-v1"
        model_dir.mkdir(parents=True, exist_ok=True)
        action_model = ActionTower().eval()
        context_model = ContextTower().eval()
        torch.onnx.export(
            action_model,
            (
                torch.ones((1, 1), dtype=torch.long),
                torch.ones((1, 1), dtype=torch.float32),
                torch.ones((1, 1), dtype=torch.bool),
            ),
            model_dir / "action_tower_github.onnx",
            input_names=["indices", "weights", "mask"],
            output_names=["embedding"],
            opset_version=18,
        )
        torch.onnx.export(
            context_model,
            (
                torch.ones((1, 1, 1), dtype=torch.long),
                torch.ones((1, 1, 1), dtype=torch.bool),
                torch.ones((1,), dtype=torch.long),
                torch.ones((1, 4, 1), dtype=torch.long),
                torch.ones((1, 4, 1), dtype=torch.float32),
                torch.ones((1, 4, 1), dtype=torch.bool),
                torch.zeros((1,), dtype=torch.long),
                torch.zeros((1,), dtype=torch.long),
                torch.zeros((1,), dtype=torch.long),
                torch.zeros((1,), dtype=torch.long),
            ),
            model_dir / "context_tower.onnx",
            input_names=[
                "hist_window_indices",
                "hist_window_mask",
                "hist_num_windows",
                "peer_indices",
                "peer_weights",
                "peer_mask",
                "role",
                "location",
                "duration",
                "privilege",
            ],
            output_names=["embedding"],
            opset_version=18,
        )
        (model_dir / "principal-vocab.json").write_text(json.dumps(["enk_alice@example.com"]), encoding="utf-8")
        (model_dir / "resource-vocab.json").write_text(json.dumps(["enk_repo-secret"]), encoding="utf-8")
        (model_dir / "categorical-vocabs.json").write_text(
            json.dumps(
                {
                    "role": {"staff-engineer": 0},
                    "location": {"unknown": 0},
                    "duration_bucket": {"1-3yr": 0},
                    "privilege_level": {"elevated": 0},
                }
            ),
            encoding="utf-8",
        )
        write_json(
            model_dir / "metadata.json",
            {
                "artifact_version": 1,
                "tenant_id": self.workspace.tenant_id,
                "model_id": "demo-v1",
                "model_version": "demo-v1",
                "model_family": "contrastive-facade",
                "scoring_mode": "onnx",
                "feature_schema_version": "feature.v1",
                "padding": {"max_tokens": 1, "max_windows": 1, "max_res_per_window": 1, "max_peers": 1},
                "action_towers": {
                    "github": {
                        "path": "action_tower_github.onnx",
                        "sha256": "sha256:test",
                        "input_names": ["indices", "weights", "mask"],
                        "output_name": "embedding",
                    }
                },
                "context_tower": {
                    "path": "context_tower.onnx",
                    "sha256": "sha256:test",
                    "input_names": [
                        "hist_window_indices",
                        "hist_window_mask",
                        "hist_num_windows",
                        "peer_indices",
                        "peer_weights",
                        "peer_mask",
                        "role",
                        "location",
                        "duration",
                        "privilege",
                    ],
                    "output_name": "embedding",
                },
                "eval_report_path": "eval-report.json",
                "score_policy": {
                    "detection_threshold": 0.8,
                    "high_severity_threshold": 0.9,
                    "calibration_source": "heldout",
                    "calibration_reason": "global_threshold_for_test",
                    "source_policies": {
                        "github": {
                            "detection_threshold": 0.35,
                            "high_severity_threshold": 0.7,
                            "calibration_source": "heldout:github",
                            "calibration_reason": "source_specific_threshold_for_test",
                            "evaluation_pair_count": 4,
                        }
                    },
                },
                "input_vocabs": {
                    "principal_entity_keys_path": "principal-vocab.json",
                    "resource_entity_keys_path": "resource-vocab.json",
                    "categorical_vocabs_path": "categorical-vocabs.json",
                    "principal_vocab_count": 1,
                    "resource_vocab_count": 1,
                    "categorical_vocab_counts": {
                        "role": 1,
                        "location": 1,
                        "duration_bucket": 1,
                        "privilege_level": 1,
                    },
                },
            },
        )
        self.workspace.save_model_artifact_manifest(
            {
                "manifest_version": 1,
                "active_model_id": "demo-v1",
                "active_metadata_path": "models/demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "test",
            }
        )

        result = run_detection_worker(self.workspace)
        detections = self.workspace.list_detections()

        self.assertEqual(result["scoring_runtime"]["effective_mode"], "onnx_native")
        self.assertEqual(result["detect"]["new_detection_count"], 1)
        self.assertEqual(len(detections), 1)
        self.assertEqual(detections[0]["model_version"], "demo-v1")
        self.assertIn("model_distance", detections[0]["feature_attributions"])
        self.assertEqual(detections[0]["model_rationale"]["policy_scope"], "github")
        self.assertAlmostEqual(detections[0]["model_rationale"]["detection_threshold"], 0.35)
        self.assertGreater(detections[0]["model_rationale"]["score_margin"], 0)

    def test_model_training_export_install_and_detection_detail_rationale_end_to_end(self) -> None:
        seed_workspace(self.workspace)
        ingest_raw_events(self.workspace)
        run_feature_builder(self.workspace)

        training = export_workspace_model_artifact(
            self.workspace,
            self.tempdir / "trained-model",
            model_id="acceptance-v1",
            epochs=1,
            install=False,
        )
        install_model_artifact_bundle(
            self.workspace,
            Path(training.output_dir),
            activation_source="acceptance_test",
            force=True,
        )

        result = run_detection_worker(self.workspace)
        runtime = build_model_runtime_status(self.workspace)
        detections = self.workspace.list_detections()
        detail = get_detection_detail(self.workspace, detections[0]["detection_id"])

        self.assertEqual(runtime["effective_mode"], "onnx_native")
        self.assertGreater(result["detect"]["new_detection_count"], 0)
        self.assertTrue(detections)
        self.assertIsNotNone(detail["detection"]["model_rationale"])
        self.assertEqual(detail["detection"]["model_rationale"]["scoring_mode"], "onnx_native")
        self.assertIn(
            detail["detection"]["model_rationale"]["policy_scope"],
            {"global", detail["events"][0]["source"]},
        )
        self.assertGreaterEqual(detail["detection"]["model_rationale"]["score_margin"], 0.0)

    def test_export_workspace_model_artifact_requires_feature_lake(self) -> None:
        with self.assertRaisesRegex(ValueError, "feature lake is not ready"):
            export_workspace_model_artifact(
                self.workspace,
                self.tempdir / "model-bundle",
                model_id="contrastive-demo-v1",
            )

    def test_model_runtime_status_requires_vocab_artifacts(self) -> None:
        model_dir = self.workspace.models_dir / "contrastive-demo-v1"
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / "action_tower_okta.onnx").write_bytes(b"onnx")
        (model_dir / "context_tower.onnx").write_bytes(b"onnx")
        (model_dir / "metadata.json").write_text(
            json.dumps(
                {
                    "artifact_version": 1,
                    "tenant_id": self.workspace.tenant_id,
                    "model_id": "contrastive-demo-v1",
                    "model_version": "contrastive-demo-v1",
                    "model_family": "contrastive-facade",
                    "scoring_mode": "onnx",
                    "feature_schema_version": "feature.v1",
                    "action_towers": {
                        "okta": {
                            "path": "action_tower_okta.onnx",
                            "sha256": "sha256:test",
                            "input_names": ["indices", "weights", "mask"],
                            "output_name": "embedding",
                        }
                    },
                    "context_tower": {
                        "path": "context_tower.onnx",
                        "sha256": "sha256:test",
                        "input_names": ["hist_window_indices"],
                        "output_name": "embedding",
                    },
                    "eval_report_path": "eval-report.json",
                    "input_vocabs": {},
                }
            ),
            encoding="utf-8",
        )
        self.workspace.save_model_artifact_manifest(
            {
                "manifest_version": 1,
                "active_model_id": "contrastive-demo-v1",
                "active_metadata_path": "models/contrastive-demo-v1/metadata.json",
                "activated_at": "2026-03-17T00:00:00Z",
                "activation_source": "test",
            }
        )

        runtime = build_model_runtime_status(self.workspace)

        self.assertFalse(runtime["available"])
        self.assertEqual(runtime["requested_mode"], "onnx")
        self.assertEqual(runtime["reason"], "model_artifact_incomplete")

    def test_rebuild_derived_state_reuses_cached_detection_context(self) -> None:
        run_runtime(self.workspace)
        build_detection_context(self.workspace)

        with patch(
            "seccloud.detection_context.build_detection_context",
            side_effect=AssertionError("should not rebuild detection context"),
        ):
            rebuilt = rebuild_derived_state(self.workspace)

        self.assertGreater(rebuilt["normalized_event_count"], 0)

    def test_runtime_produces_expected_detection_types(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        scenarios = {item["scenario"] for item in detections}

        expected_scenarios = {
            "compromised_privileged_identity",
            "unusual_repo_export",
            "unusual_data_access",
            "unusual_external_sharing",
        }
        self.assertTrue(
            expected_scenarios.issubset(scenarios),
            f"Missing expected scenarios: {expected_scenarios - scenarios}",
        )
        self.assertGreaterEqual(len(detections), len(expected_scenarios))
        self.assertNotIn("general_behavioral_anomaly", scenarios)

    def test_case_workflow_and_timeline(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        case = create_case_from_detection(self.workspace, detection["detection_id"])
        updated = update_case(
            self.workspace,
            case["case_id"],
            disposition="escalated",
            analyst_note="Needs review",
            feedback_label="high_value_signal",
        )
        timeline = get_entity_timeline(self.workspace, principal_id=detection["related_entity_ids"][0])
        evidence = build_evidence_bundle(self.workspace, detection["detection_id"])
        peer_comparison = build_peer_comparison(self.workspace, detection["detection_id"])
        summary = summarize_case(self.workspace, case["case_id"])

        self.assertEqual(updated["disposition"], "escalated")
        self.assertIn("high_value_signal", updated["feedback_labels"])
        self.assertTrue(timeline)
        self.assertTrue(evidence["evidence_items"])
        self.assertEqual(peer_comparison["principal_id"], detection["related_entity_ids"][0])
        self.assertGreater(peer_comparison["peer_group_principal_count"], 0)
        self.assertIn("principal_role", peer_comparison)
        self.assertIn("principal_privilege_level", peer_comparison)
        self.assertIn("principal_prior_event_count", peer_comparison)
        self.assertIn("principal_prior_action_count", peer_comparison)
        self.assertIsInstance(peer_comparison["geo_seen_before"], bool)
        self.assertEqual(summary["case_id"], case["case_id"])
        self.assertEqual(summary["primary_detection"]["detection_id"], detection["detection_id"])
        self.assertTrue(summary["case_title"])

    def test_acknowledge_detection_reduces_open_detection_count(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]

        before = get_runtime_stream_state(self.workspace)
        updated = acknowledge_detection(self.workspace, detection["detection_id"])
        after = get_runtime_stream_state(self.workspace)

        self.assertEqual(updated["status"], "acknowledged")
        self.assertEqual(after["detection_count"], before["detection_count"] - 1)

    def test_peer_comparison_uses_feature_lake_when_derived_state_is_empty(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        self.workspace.save_derived_state({"metadata": {}})
        anchor_event = next(
            event for event in self.workspace.list_normalized_events() if event["event_id"] == detection["event_ids"][0]
        )

        with (
            patch("seccloud.investigation.get_event_detail", return_value=anchor_event),
            patch.object(
                self.workspace,
                "list_normalized_events",
                side_effect=AssertionError("should not scan events"),
            ),
        ):
            peer_comparison = build_peer_comparison(self.workspace, detection["detection_id"])

        self.assertEqual(peer_comparison["principal_id"], detection["related_entity_ids"][0])
        self.assertGreater(peer_comparison["principal_total_events"], 0)
        self.assertGreater(peer_comparison["peer_group_principal_count"], 0)
        self.assertGreaterEqual(peer_comparison["department_peer_count"], 0)
        self.assertGreaterEqual(peer_comparison["peer_group_resource_principal_count"], 1)
        self.assertGreaterEqual(peer_comparison["principal_prior_event_count"], 0)
        self.assertGreaterEqual(peer_comparison["principal_prior_action_count"], 0)

    def test_peer_comparison_falls_back_to_event_scan_without_feature_lake_or_derived_state(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        self.workspace.save_derived_state({"metadata": {}})
        self.workspace.feature_state_path.write_text(
            json.dumps(
                {
                    "input_signature": "",
                    "normalized_event_count": 0,
                    "action_feature_row_count": 0,
                    "history_feature_row_count": 0,
                    "collaboration_feature_row_count": 0,
                    "static_feature_row_count": 0,
                    "peer_group_feature_row_count": 0,
                    "action_manifest_key": None,
                    "history_manifest_key": None,
                    "collaboration_manifest_key": None,
                    "static_manifest_key": None,
                    "peer_group_manifest_key": None,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        anchor_event = next(
            event for event in self.workspace.list_normalized_events() if event["event_id"] == detection["event_ids"][0]
        )

        with (
            patch("seccloud.investigation.get_event_detail", return_value=anchor_event),
            patch.object(
                self.workspace,
                "load_derived_state",
                side_effect=AssertionError("should not load derived state"),
            ),
        ):
            peer_comparison = build_peer_comparison(self.workspace, detection["detection_id"])

        self.assertEqual(peer_comparison["principal_id"], detection["related_entity_ids"][0])
        self.assertGreater(peer_comparison["principal_total_events"], 0)
        self.assertGreater(peer_comparison["peer_group_principal_count"], 0)
        self.assertGreaterEqual(peer_comparison["principal_prior_event_count"], 0)

    def test_peer_comparison_prior_baseline_comes_from_detection_context(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        peer_comparison = build_peer_comparison(self.workspace, detection["detection_id"])
        event_id = detection["event_ids"][0]
        context = ensure_detection_context(self.workspace)["contexts_by_event_id"][event_id]

        self.assertEqual(
            peer_comparison["principal_prior_event_count"],
            context["principal_total_events"],
        )
        self.assertEqual(
            peer_comparison["principal_prior_action_count"],
            context["action_count"],
        )
        self.assertEqual(
            peer_comparison["principal_prior_resource_access_count"],
            context["resource_count"],
        )
        self.assertEqual(
            peer_comparison["peer_group_resource_access_count"],
            context["peer_resource_count"],
        )

    def test_local_event_detail_timeline_and_case_flow_use_event_index(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]
        anchor_event = get_event_detail(self.workspace, detection["event_ids"][0])
        self.assertIsNotNone(anchor_event)

        with patch.object(
            self.workspace,
            "list_normalized_events",
            side_effect=AssertionError("should not scan normalized events"),
        ):
            detail = get_event_detail(self.workspace, detection["event_ids"][0])
            timeline = get_entity_timeline(self.workspace, principal_id=detection["related_entity_ids"][0])
            case = create_case_from_detection(self.workspace, detection["detection_id"])

        self.assertEqual(detail["event_id"], detection["event_ids"][0])
        self.assertTrue(timeline)
        self.assertIn(detection["event_ids"][0], case["timeline_event_ids"])

    def test_source_capability_matrix_includes_last_seen_signals(self) -> None:
        run_runtime(self.workspace)

        matrix = build_source_capability_matrix(self.workspace)
        okta = matrix["sources"]["okta"]
        source_stats = self.workspace.load_source_stats()["sources"]["okta"]

        self.assertIsNotNone(okta["recent_window_anchor_at"])
        self.assertIsNotNone(okta["raw_last_seen_at"])
        self.assertIsNotNone(okta["normalized_last_seen_at"])
        self.assertGreater(okta["raw_24h_count"], 0)
        self.assertGreater(okta["normalized_24h_count"], 0)
        self.assertGreaterEqual(okta["dead_letter_7d_count"], 0)
        self.assertIn("login", okta["seen_event_types"])
        self.assertGreater(source_stats["raw_event_count"], 0)
        self.assertGreater(source_stats["normalized_event_count"], 0)
        self.assertIn("event_type", source_stats["seen_raw_fields"])

    def test_related_detections_group_into_one_case(self) -> None:
        run_runtime(self.workspace)
        detections = list_detections(self.workspace)
        alice_detections = [item for item in detections if "alice@example.com" in item["related_entity_ids"]]

        first_case = create_case_from_detection(self.workspace, alice_detections[0]["detection_id"])
        grouped_case = create_case_from_detection(self.workspace, alice_detections[1]["detection_id"])
        summary = summarize_case(self.workspace, first_case["case_id"])

        self.assertEqual(first_case["case_id"], grouped_case["case_id"])
        self.assertEqual(len(grouped_case["detection_ids"]), 2)
        self.assertEqual(summary["detection_count"], 2)

    def test_run_runtime_is_repeatable_and_removes_stale_detections(self) -> None:
        first = run_runtime(self.workspace)
        second = run_runtime(self.workspace)

        self.assertEqual(
            first["detect"]["total_detection_count"],
            second["detect"]["total_detection_count"],
        )
        self.assertEqual(
            second["detect"]["new_detection_count"],
            second["detect"]["total_detection_count"],
        )
        self.assertNotIn("evaluation", first)
        self.assertNotIn("evaluation", second)

    def test_runtime_stream_spreads_scenarios_across_multiple_advances(self) -> None:
        initialize_runtime_stream(self.workspace)
        manifest = json.loads((self.workspace.manifests_dir / "runtime_stream_source_events.json").read_text())
        source_events = manifest["source_events"]
        nonbaseline_positions = [
            index for index, event in enumerate(source_events, start=1) if event.get("scenario") != "baseline"
        ]

        self.assertTrue(nonbaseline_positions)
        self.assertLess(nonbaseline_positions[0], 80)
        self.assertLess(nonbaseline_positions[-1], len(source_events))

        detection_advances: list[int] = []
        while True:
            advance = advance_runtime_stream(self.workspace, batch_size=25)
            worker_result = run_local_processing_workers(self.workspace)
            if worker_result["detect"]["new_detection_count"] > 0:
                detection_advances.append(advance["cursor"])
            if advance["complete"]:
                break

        self.assertGreaterEqual(len(detection_advances), 3)
        self.assertLess(detection_advances[0], 125)

    def test_ops_metadata_contains_no_raw_payloads(self) -> None:
        run_runtime(self.workspace)
        metadata = collect_ops_metadata(self.workspace)
        serialized = json.dumps(metadata)

        self.assertFalse(metadata["contains_raw_payloads"])
        self.assertNotIn("normalized_event_count", metadata)
        self.assertNotIn("detection_count", metadata)
        self.assertNotIn("actor_email", serialized)
        self.assertNotIn("resource_id", serialized)

    def test_founder_artifacts_include_conversation_pack_and_capability_matrix(self) -> None:
        run_runtime(self.workspace)
        exported = export_founder_artifacts(self.workspace)
        conversation_pack = build_conversation_pack_markdown(self.workspace)
        capability_matrix = build_source_capability_markdown(self.workspace)
        capability_data = build_source_capability_matrix(self.workspace)

        self.assertIn("conversation-pack.md", exported)
        self.assertIn("source-capability-matrix.md", exported)
        self.assertNotIn("evaluation-summary.md", exported)
        self.assertNotIn("scenario-outputs.md", exported)
        self.assertIn("What The Current PoC Proves", conversation_pack)
        self.assertIn("Total detections: `5`", conversation_pack)
        self.assertIn("Dead letters: `2`", conversation_pack)
        self.assertIn("Source contracts fully satisfied: `2/4`", conversation_pack)
        self.assertIn("Durable feature tables materialized:", conversation_pack)
        self.assertIn("Scoring input mode:", conversation_pack)
        self.assertIn("Principals in feature vocab:", conversation_pack)
        self.assertIn("Indexed normalized events:", conversation_pack)
        self.assertIn("Indexed principals/resources/departments:", conversation_pack)
        self.assertIn("Detection context materialized:", conversation_pack)
        self.assertIn("Identity profiles loaded:", conversation_pack)
        self.assertIn("Projection available:", conversation_pack)
        self.assertIn("Projected normalized events/detections:", conversation_pack)
        self.assertIn("## Capability Status", capability_matrix)
        self.assertEqual(capability_data["sources"]["okta"]["dead_letter_count"], 1)
        self.assertEqual(capability_data["sources"]["snowflake"]["missing_required_event_types"], [])
        self.assertTrue((self.workspace.founder_dir / "conversation-pack.md").exists())
        self.assertTrue((self.workspace.founder_dir / "source-capability-matrix.md").exists())

    def test_local_demo_paths_work_without_normalized_json_segments(self) -> None:
        run_runtime(self.workspace)
        shutil.rmtree(self.workspace.normalized_dir)
        self.workspace.save_source_stats({"sources": {}})

        detections = list_detections(self.workspace)
        detection = detections[0]
        event = get_event_detail(self.workspace, detection["event_ids"][0])
        timeline = get_entity_timeline(self.workspace, principal_id=detection["related_entity_ids"][0])
        case = create_case_from_detection(self.workspace, detection["detection_id"])
        stats = run_source_stats_projector(self.workspace)
        status = build_operator_runtime_status(
            self.workspace,
            dsn="postgresql://projection",
            runtime_root=self.tempdir,
        )
        conversation_pack = build_conversation_pack_markdown(self.workspace)

        self.assertIsNotNone(event)
        self.assertTrue(timeline)
        self.assertIn(detection["event_ids"][0], case["timeline_event_ids"])
        self.assertGreaterEqual(stats["source_count"], 1)
        self.assertGreaterEqual(status["event_index"]["event_count"], 1)
        self.assertIn("Indexed normalized events:", conversation_pack)

    def test_source_fixture_bundle_validation_and_import(self) -> None:
        fixtures_dir = ROOT / "examples" / "poc" / "fixtures" / "fixed-source-pack"
        validation = validate_fixture_bundle(fixtures_dir)
        report = build_onboarding_report_markdown(fixtures_dir)
        imported = import_fixture_bundle(self.workspace, fixtures_dir)
        worker_result = run_local_processing_workers(self.workspace)
        manifest = build_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["valid_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(imported["batch_count"], 4)
        self.assertEqual(worker_result["normalization"]["ingest"]["normalized_events_seen"], 8)
        self.assertEqual(manifest["source_pack"], ["okta", "gworkspace", "github", "snowflake"])
        self.assertIn("Validation status: `pass`", report)

    def test_source_fixture_bundle_validation_fails_for_missing_sources(self) -> None:
        fixtures_dir = self.tempdir / "fixtures"
        fixtures_dir.mkdir()
        (fixtures_dir / "okta.jsonl").write_text(
            json.dumps(
                {
                    "source": "okta",
                    "source_event_id": "okta-only-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        validation = validate_fixture_bundle(fixtures_dir)

        self.assertFalse(validation["summary"]["passes"])
        self.assertIn("gworkspace", validation["summary"]["missing_sources"])
        self.assertIn("github", validation["summary"]["missing_sources"])
        self.assertIn("snowflake", validation["summary"]["missing_sources"])

    def test_vendor_fixture_bundle_validation_and_import(self) -> None:
        fixtures_dir = ROOT / "examples" / "poc" / "vendor-fixtures" / "fixed-source-pack"
        validation = validate_vendor_fixture_bundle(fixtures_dir)
        report = build_vendor_mapping_report_markdown(fixtures_dir)
        imported = import_vendor_fixture_bundle(self.workspace, fixtures_dir)
        worker_result = run_local_processing_workers(self.workspace)
        manifest = build_vendor_source_manifest()

        self.assertTrue(validation["summary"]["passes"])
        self.assertEqual(validation["summary"]["mapped_event_count"], 8)
        self.assertEqual(imported["imported_event_count"], 8)
        self.assertEqual(imported["skipped_invalid_event_count"], 0)
        self.assertEqual(imported["batch_count"], 4)
        self.assertEqual(worker_result["normalization"]["ingest"]["normalized_events_seen"], 8)
        self.assertEqual(manifest["mapping_target"], "raw-event-v1")
        self.assertIn("Validation status: `pass`", report)

    def test_vendor_fixture_bundle_validation_fails_for_unsupported_event(self) -> None:
        fixtures_dir = self.tempdir / "vendor-fixtures"
        fixtures_dir.mkdir()
        (fixtures_dir / "okta_system_log.jsonl").write_text(
            json.dumps(
                {
                    "uuid": "okta-bad-0001",
                    "published": "2026-02-12T10:00:00Z",
                    "ingested_at": "2026-02-12T10:01:00Z",
                    "eventType": "user.mfa.challenge",
                    "actor": {
                        "alternateId": "alice@example.com",
                        "displayName": "Alice Admin",
                        "department": "security",
                        "role": "security-admin",
                    },
                    "client": {
                        "ipAddress": "10.0.0.1",
                        "geographicalContext": {"country": "US", "state": "NY"},
                    },
                    "target": [{"id": "okta:admin-console", "displayName": "Admin Console", "type": "app"}],
                    "securityContext": {"isPrivileged": True},
                    "debugContext": {"sensitivity": "high"},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        validation = validate_vendor_fixture_bundle(fixtures_dir)

        self.assertFalse(validation["summary"]["passes"])
        self.assertEqual(validation["sources"]["okta"]["invalid_event_count"], 1)
        self.assertIn("gworkspace", validation["summary"]["missing_sources"])

    def test_cleanup_stale_legacy_postgres_runtime_files(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("999999\n", encoding="utf-8")
        (paths["socket"] / ".s.PGSQL.55432").write_text("", encoding="utf-8")
        (paths["socket"] / ".s.PGSQL.55432.lock").write_text("", encoding="utf-8")

        with patch("seccloud.local_postgres._process_exists", return_value=False):
            removed = _cleanup_stale_runtime_files(paths)

        self.assertEqual(len(removed), 3)
        self.assertFalse((paths["data"] / "postmaster.pid").exists())
        self.assertFalse((paths["socket"] / ".s.PGSQL.55432").exists())
        self.assertFalse((paths["socket"] / ".s.PGSQL.55432.lock").exists())

    def test_cleanup_stale_legacy_postgres_refuses_live_pid(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("12345\n", encoding="utf-8")

        with (
            patch("seccloud.local_postgres._process_exists", return_value=True),
            patch(
                "seccloud.local_postgres._process_command",
                return_value=f"postgres -D {paths['data']}",
            ),
        ):
            with self.assertRaises(RuntimeError):
                _cleanup_stale_runtime_files(paths)

    def test_cleanup_stale_legacy_postgres_allows_reused_pid(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["data"].mkdir(parents=True)
        paths["socket"].mkdir(parents=True)
        (paths["data"] / "postmaster.pid").write_text("12345\n", encoding="utf-8")

        with (
            patch("seccloud.local_postgres._process_exists", return_value=True),
            patch(
                "seccloud.local_postgres._process_command",
                return_value="python some_other_process.py",
            ),
        ):
            removed = _cleanup_stale_runtime_files(paths)

        self.assertEqual(len(removed), 1)
        self.assertFalse((paths["data"] / "postmaster.pid").exists())

    def test_wait_for_socket_ready_succeeds_when_socket_exists(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["socket"].mkdir(parents=True)
        (paths["socket"] / ".s.PGSQL.55432").write_text("", encoding="utf-8")

        _wait_for_socket_ready(paths, timeout_seconds=0.01, poll_interval_seconds=0.001)

    def test_wait_for_socket_ready_times_out_when_socket_missing(self) -> None:
        paths = postgres_paths(self.tempdir)
        paths["socket"].mkdir(parents=True)

        with self.assertRaises(RuntimeError):
            _wait_for_socket_ready(paths, timeout_seconds=0.01, poll_interval_seconds=0.001)

    def test_start_local_postgres_recovers_from_stale_running_status(self) -> None:
        completed = subprocess.CompletedProcess(args=["pg_ctl"], returncode=0, stdout="", stderr="")

        with (
            patch("seccloud.local_postgres.init_local_postgres", return_value={"status": "already_initialized"}),
            patch("seccloud.local_postgres._run_pg_ctl", return_value=completed),
            patch(
                "seccloud.local_postgres._wait_for_socket_ready",
                side_effect=[RuntimeError("Local Postgres did not create its socket within 1.0s"), None],
            ),
            patch("seccloud.local_postgres._stop_pg_ctl"),
            patch("seccloud.local_postgres._cleanup_stale_runtime_files", return_value=["stale.pid"]),
            patch("seccloud.local_postgres._ensure_database", return_value="already_exists"),
            patch("seccloud.local_postgres._postgres_binary", return_value="/opt/homebrew/bin/pg_ctl"),
            patch("seccloud.local_postgres.subprocess.run"),
        ):
            from seccloud.local_postgres import start_local_postgres

            result = start_local_postgres(self.tempdir)

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["database_status"], "already_exists")
        self.assertEqual(result["stale_files_removed"], ["stale.pid"])

    def test_runtime_stream_advances_incrementally(self) -> None:
        initialized = initialize_runtime_stream(self.workspace)
        after_first = advance_runtime_stream(self.workspace, batch_size=7)
        run_local_processing_workers(self.workspace)
        state_after_first = get_runtime_stream_state(self.workspace)
        after_second = advance_runtime_stream(self.workspace, batch_size=7)

        self.assertEqual(initialized["cursor"], 0)
        self.assertEqual(after_first["cursor"], 7)
        self.assertEqual(after_first["accepted_records"], 7)
        self.assertEqual(state_after_first["normalized_event_count"], 7)
        self.assertEqual(state_after_first["detection_count"], len(self.workspace.list_detections()))
        self.assertGreaterEqual(after_second["cursor"], 14)

    def test_continuous_runtime_stream_advances_without_exhaustion(self) -> None:
        initialized = initialize_runtime_stream(
            self.workspace,
            scaled=True,
            continuous=True,
            num_principals=20,
            seed=7,
        )
        first = advance_runtime_stream(self.workspace, batch_size=25)
        second = advance_runtime_stream(self.workspace, batch_size=25)
        state = get_runtime_stream_state(self.workspace)
        manifest = json.loads((self.workspace.manifests_dir / "runtime_stream_manifest.json").read_text())

        self.assertEqual(initialized["mode"], "continuous")
        self.assertEqual(first["accepted_records"], 25)
        self.assertEqual(second["accepted_records"], 25)
        self.assertFalse(first["complete"])
        self.assertFalse(second["complete"])
        self.assertFalse(state["complete"])
        self.assertGreaterEqual(state["cursor"], 50)
        self.assertGreaterEqual(state["total_source_events"], state["cursor"])
        self.assertEqual(manifest["mode"], "continuous")
        self.assertGreaterEqual(manifest["simulation_day_offset"], 1)
        self.assertFalse((self.workspace.manifests_dir / "precomputed_detections.json").exists())

    def test_cli_exposes_service_and_materialization_worker_commands(self) -> None:
        parser = build_parser()
        subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))

        for command in (
            "run-pipeline",
            "run-normalization-worker",
            "run-detection-worker",
            "run-projector-worker",
            "run-workers",
            "run-all-workers",
            "sync-projection",
        ):
            self.assertNotIn(command, subparsers.choices)

        for command in (
            "run-worker-service",
            "run-worker-service-once",
            "run-demo-stream",
            "run-source-stats-projector",
            "run-feature-builder",
            "list-model-artifacts",
            "activate-model-artifact",
            "deactivate-model-artifact",
            "rollback-model-artifact",
            "show-worker-state",
            "show-runtime-status",
            "bootstrap-local-runtime",
            "run-api",
        ):
            self.assertIn(command, subparsers.choices)

    def test_run_demo_stream_advances_until_max_steps(self) -> None:
        initialize_runtime_stream(self.workspace)

        result = run_demo_stream(
            self.workspace,
            batch_size=7,
            interval_seconds=0,
            max_steps=2,
        )

        self.assertEqual(result["status"], "stopped")
        self.assertEqual(result["steps"], 2)
        self.assertGreater(result["accepted_records"], 0)
        self.assertEqual(result["stream_state"]["cursor"], 14)

    def test_run_demo_stream_stays_live_in_continuous_mode(self) -> None:
        initialize_runtime_stream(
            self.workspace,
            scaled=True,
            continuous=True,
            num_principals=20,
            seed=11,
        )

        result = run_demo_stream(
            self.workspace,
            batch_size=30,
            interval_seconds=0,
            max_steps=2,
        )

        self.assertEqual(result["status"], "stopped")
        self.assertEqual(result["steps"], 2)
        self.assertGreater(result["accepted_records"], 0)
        self.assertFalse(result["stream_state"]["complete"])
        self.assertGreaterEqual(result["stream_state"]["cursor"], 60)

    def test_workers_process_shared_intake_batches(self) -> None:
        submitted = submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-worker-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                },
                {
                    "source": "github",
                    "source_event_id": "github-worker-0001",
                    "observed_at": "2026-02-11T10:05:00Z",
                    "received_at": "2026-02-11T10:06:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "archive_download",
                    "resource_id": "github:repo/seccloud",
                    "resource_name": "seccloud",
                    "resource_kind": "repo",
                    "sensitivity": "internal",
                    "bytes_transferred_mb": 44,
                },
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )

        self.assertEqual(submitted["batch_count"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 2)

        normalization = run_normalization_worker(self.workspace)
        detection = run_local_processing_workers(self.workspace)
        worker_state = get_worker_state(self.workspace)

        self.assertEqual(normalization["processed_batch_count"], 2)
        self.assertEqual(len(self.workspace.list_pending_intake_batches()), 0)
        self.assertEqual(len(self.workspace.list_processed_intake_batches()), 2)
        self.assertEqual(normalization["ingest"]["added_normalized_events"], 2)
        self.assertEqual(detection["normalization"]["processed_batch_count"], 0)
        self.assertIn("detection_context", detection)
        self.assertGreaterEqual(detection["detection_context"]["event_count"], 2)
        self.assertEqual(worker_state["pending_batch_count"], 0)
        self.assertEqual(worker_state["processed_batch_count"], 2)
        self.assertGreaterEqual(worker_state["normalization_runs"], 2)
        self.assertGreaterEqual(worker_state["detection_runs"], 1)

    def test_event_identity_and_object_keys_are_partition_aware(self) -> None:
        tenant_workspace = Workspace(self.tempdir / "tenant-workspace", tenant_id="acme-prod")
        tenant_workspace.bootstrap()

        submit_grouped_raw_events(
            tenant_workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-tenant-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            ],
            intake_kind="push_gateway",
            integration_id="okta-prod",
        )
        run_local_processing_workers(tenant_workspace)

        event = tenant_workspace.list_normalized_events()[0]
        self.assertEqual(event["event_id"], f"event::{event['event_key']}")
        self.assertTrue(event["event_key"].startswith("evk_"))
        self.assertEqual(event["integration_id"], "okta-prod")
        self.assertEqual(
            event["principal"]["entity_id"],
            f"entity::{event['principal']['entity_key']}",
        )
        self.assertTrue(event["principal"]["entity_key"].startswith("enk_"))
        self.assertEqual(
            event["resource"]["entity_id"],
            f"entity::{event['resource']['entity_key']}",
        )
        self.assertTrue(event["resource"]["entity_key"].startswith("enk_"))
        self.assertIn("tenant=acme-prod", event["evidence"]["object_key"])
        self.assertIn("integration=okta-prod", event["evidence"]["object_key"])

    def test_api_startup_does_not_sync_projection_and_read_routes_do_not_resync(self) -> None:
        overview_payload = {
            "stream_state": {
                "cursor": 0,
                "total_source_events": 0,
                "complete": False,
                "normalized_event_count": 0,
                "detection_count": 0,
            },
            "ops_metadata": {
                "workspace": str(self.workspace.root),
                "event_counts_by_source": {},
                "dead_letter_count": 0,
                "dead_letter_counts_by_source": {},
                "contains_raw_payloads": False,
            },
        }
        events_payload = {
            "items": [],
            "page": {
                "limit": 14,
                "offset": 0,
                "returned": 0,
                "total": 0,
                "has_more": False,
            },
        }

        with (
            patch.dict(
                os.environ,
                {
                    "SECCLOUD_WORKSPACE": str(self.workspace.root),
                    "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                },
                clear=False,
            ),
            patch("seccloud.api.fetch_projection_overview", return_value=overview_payload),
            patch("seccloud.api.fetch_projected_events", return_value=events_payload),
        ):
            app = seccloud_api.create_app()
            overview_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/overview"
            )
            events_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/events"
            )

            overview_response = overview_endpoint()
            events_response = events_endpoint(limit=14, offset=0)

        self.assertEqual(overview_response, overview_payload)
        self.assertEqual(events_response, events_payload)

    def test_api_runtime_status_route_returns_operator_runtime_status(self) -> None:
        status_payload = {
            "tenant_id": self.workspace.tenant_id,
            "worker_state": {
                "normalization_runs": 0,
                "feature_runs": 0,
                "detection_runs": 0,
                "source_stats_runs": 0,
                "projection_runs": 0,
                "service_runs": 0,
                "last_submitted_batch_id": None,
                "last_processed_batch_id": None,
                "last_normalization_at": None,
                "last_feature_at": None,
                "last_detection_at": None,
                "last_source_stats_at": None,
                "last_projection_at": None,
                "last_service_at": None,
                "last_service_status": None,
                "pending_batch_count": 0,
                "processed_batch_count": 0,
            },
            "feature_tables": {
                "action_row_count": 0,
                "history_row_count": 0,
                "collaboration_row_count": 0,
                "static_row_count": 0,
                "peer_group_row_count": 0,
            },
            "feature_vocab": {
                "principal_count": 0,
                "resource_count": 0,
            },
            "scoring_input": {
                "ready": False,
                "mode": "python_feature_pipeline_fallback",
                "reason": "missing_tables:action,history,static,peer_group",
                "materialized_table_count": 0,
                "materialized_tables": [],
            },
            "model_runtime": {
                "available": False,
                "requested_mode": "heuristic",
                "effective_mode": "heuristic",
                "reason": "no_active_model",
                "model_id": None,
                "model_version": None,
                "model_family": None,
                "exported_at": None,
                "metadata_path": None,
                "activated_at": None,
                "activation_source": None,
                "action_source_count": 0,
                "principal_vocab_count": 0,
                "resource_vocab_count": 0,
                "activation_gate": {
                    "eligible": False,
                    "reason": "no_active_model",
                    "status": None,
                    "training_pair_count": 0,
                    "final_loss": None,
                },
                "recent_activation_history": [],
                "installed_model_count": 0,
                "installed_model_ids": [],
            },
            "event_index": {
                "available": False,
                "event_count": 0,
                "principal_key_count": 0,
                "resource_key_count": 0,
                "department_count": 0,
                "input_signature": None,
            },
            "detection_context": {
                "available": False,
                "event_count": 0,
                "input_signature": None,
                "context_version": 1,
            },
            "identity_profiles": {
                "available": False,
                "source": "unknown",
                "principal_count": 0,
                "team_count": 0,
            },
            "projection": {
                "available": True,
                "overview": {
                    "stream_state": {
                        "cursor": 0,
                        "total_source_events": 0,
                        "complete": False,
                        "normalized_event_count": 0,
                        "detection_count": 0,
                    },
                    "ops_metadata": {
                        "workspace": str(self.workspace.root),
                        "event_counts_by_source": {},
                        "dead_letter_count": 0,
                        "dead_letter_counts_by_source": {},
                        "contains_raw_payloads": False,
                    },
                },
                "error": None,
            },
        }

        with (
            patch.dict(
                os.environ,
                {
                    "SECCLOUD_WORKSPACE": str(self.workspace.root),
                    "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                },
                clear=False,
            ),
            patch("seccloud.api.build_runtime_status", return_value=status_payload),
        ):
            app = seccloud_api.create_app()
            runtime_status_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/runtime-status"
            )

            response = runtime_status_endpoint()

        self.assertEqual(response, status_payload)

    def test_api_intake_endpoint_enqueues_without_running_workers(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SECCLOUD_WORKSPACE": str(self.workspace.root),
                "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                "SECCLOUD_PUSH_AUTH_TOKENS": json.dumps(
                    {
                        "push-token": {
                            "tenant_id": self.workspace.tenant_id,
                            "source": "okta",
                            "integration_id": "okta-primary",
                        }
                    }
                ),
            },
            clear=False,
        ):
            app = seccloud_api.create_app()
            intake_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/intake/raw-events"
            )
            worker_state_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/workers/state"
            )
            body = gzip.compress(
                json.dumps(
                    {
                        "source": "okta",
                        "records": [
                            {
                                "source_event_id": "okta-api-0001",
                                "observed_at": "2026-02-11T10:00:00Z",
                                "received_at": "2026-02-11T10:01:00Z",
                                "actor_email": "alice@example.com",
                                "actor_name": "Alice Admin",
                                "department": "security",
                                "role": "security-admin",
                                "event_type": "login",
                                "resource_id": "okta:admin-console",
                                "resource_name": "Admin Console",
                                "resource_kind": "app",
                                "sensitivity": "high",
                                "geo": "US-NY",
                                "ip": "10.0.0.1",
                                "privileged": True,
                            }
                        ],
                    }
                ).encode("utf-8")
            )

            async def receive() -> dict[str, object]:
                return {"type": "http.request", "body": body, "more_body": False}

            intake_response = asyncio.run(
                intake_endpoint(
                    request=Request(
                        {
                            "type": "http",
                            "method": "POST",
                            "path": "/api/intake/raw-events",
                            "headers": [
                                (b"content-encoding", b"gzip"),
                                (b"content-type", b"application/json"),
                            ],
                            "query_string": b"",
                            "client": ("testclient", 123),
                            "server": ("testserver", 80),
                            "scheme": "http",
                            "http_version": "1.1",
                        },
                        receive,
                    ),
                    authorization="Bearer push-token",
                    idempotency_key=None,
                )
            )
            worker_state = worker_state_endpoint()

        self.assertEqual(intake_response["source"], "okta")
        self.assertEqual(intake_response["integration_id"], "okta-primary")
        self.assertEqual(intake_response["record_count"], 1)
        self.assertEqual(worker_state["pending_batch_count"], 1)
        self.assertEqual(worker_state["processed_batch_count"], 0)
        self.assertEqual(len(self.workspace.list_normalized_events()), 0)

    def test_api_worker_state_includes_feature_fields(self) -> None:
        run_runtime(self.workspace)
        run_feature_builder(self.workspace)

        with patch.dict(
            os.environ,
            {
                "SECCLOUD_WORKSPACE": str(self.workspace.root),
                "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
            },
            clear=False,
        ):
            app = seccloud_api.create_app()
            worker_state_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/workers/state"
            )
            worker_state = worker_state_endpoint()

        self.assertGreaterEqual(worker_state["feature_runs"], 1)
        self.assertIsNotNone(worker_state["last_feature_at"])

    def test_api_acknowledge_detection_updates_status(self) -> None:
        run_runtime(self.workspace)
        detection = list_detections(self.workspace)[0]

        with (
            patch.dict(
                os.environ,
                {
                    "SECCLOUD_WORKSPACE": str(self.workspace.root),
                    "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
                },
                clear=False,
            ),
            patch("seccloud.api.sync_workspace_projection", return_value={"detection_count": 0}),
        ):
            app = seccloud_api.create_app()
            acknowledge_endpoint = next(
                route.endpoint
                for route in app.routes
                if getattr(route, "path", None) == "/api/detections/{detection_id}/acknowledge"
            )
            response = acknowledge_endpoint(detection["detection_id"])

        self.assertEqual(response["status"], "acknowledged")
        self.assertEqual(
            self.workspace.get_detection(detection["detection_id"])["status"],
            "acknowledged",
        )

    def test_api_stream_advance_enqueues_without_processing(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SECCLOUD_WORKSPACE": str(self.workspace.root),
                "SECCLOUD_PROJECTION_DSN": "postgresql://projection",
            },
            clear=False,
        ):
            app = seccloud_api.create_app()
            reset_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/stream/reset"
            )
            advance_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/stream/advance"
            )
            worker_state_endpoint = next(
                route.endpoint for route in app.routes if getattr(route, "path", None) == "/api/workers/state"
            )

            reset_endpoint()
            advance_response = advance_endpoint(batch_size=10)
            worker_state = worker_state_endpoint()

        self.assertEqual(advance_response["accepted_records"], 10)
        self.assertGreaterEqual(advance_response["accepted_batches"], 1)
        self.assertEqual(worker_state["pending_batch_count"], advance_response["accepted_batches"])
        self.assertEqual(advance_response["pending_batch_count"], advance_response["accepted_batches"])
        self.assertEqual(len(self.workspace.list_normalized_events()), 0)

    def test_projector_worker_requires_rust_runtime(self) -> None:
        with patch("seccloud.workers.shutil.which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "requires `cargo`"):
                run_projector_worker(self.workspace, "postgresql://projection")

    def test_run_projector_worker_uses_rust_runtime(self) -> None:
        rust_result = {
            "dsn": "postgresql://projection",
            "event_count": 5,
            "detection_count": 2,
        }

        with patch("seccloud.workers._run_rust_projector_worker", return_value=rust_result) as rust_worker:
            result = run_projector_worker(self.workspace, "postgresql://projection")

        self.assertEqual(result, rust_result)
        rust_worker.assert_called_once_with(self.workspace, "postgresql://projection")

    def test_source_stats_projector_updates_worker_state(self) -> None:
        run_runtime(self.workspace)
        self.workspace.request_source_stats_refresh()

        stats = run_source_stats_projector(self.workspace)

        worker_state = get_worker_state(self.workspace)
        self.assertGreater(stats["source_count"], 0)
        self.assertEqual(worker_state["source_stats_runs"], 1)
        self.assertIsNotNone(worker_state["last_source_stats_at"])
        self.assertFalse(self.workspace.source_stats_refresh_requested())

    def test_source_stats_projector_rebuilds_from_event_index_without_normalized_json(self) -> None:
        run_runtime(self.workspace)
        shutil.rmtree(self.workspace.normalized_dir)
        self.workspace.save_source_stats({"sources": {}})
        self.workspace.request_source_stats_refresh()

        stats = run_source_stats_projector(self.workspace)
        source_stats = self.workspace.load_source_stats()["sources"]

        self.assertGreaterEqual(stats["source_count"], 1)
        self.assertGreater(source_stats["okta"]["normalized_event_count"], 0)

    def test_feature_builder_updates_worker_state_and_writes_manifests(self) -> None:
        run_runtime(self.workspace)
        before = get_worker_state(self.workspace)

        features = run_feature_builder(self.workspace)

        worker_state = get_worker_state(self.workspace)
        self.assertGreater(features["normalized_event_count"], 0)
        self.assertIsNotNone(features["action_manifest_key"])
        self.assertIsNotNone(features["history_manifest_key"])
        self.assertIsNotNone(features["static_manifest_key"])
        if features["peer_group_feature_row_count"] > 0:
            self.assertIsNotNone(features["peer_group_manifest_key"])
        else:
            self.assertIsNone(features["peer_group_manifest_key"])
        if features["collaboration_feature_row_count"] > 0:
            self.assertIsNotNone(features["collaboration_manifest_key"])
        else:
            self.assertIsNone(features["collaboration_manifest_key"])
        self.assertEqual(worker_state["feature_runs"], before["feature_runs"] + 1)
        self.assertIsNotNone(worker_state["last_feature_at"])

    def test_feature_lake_snapshot_matches_feature_builder_output(self) -> None:
        run_runtime(self.workspace)

        features = run_feature_builder(self.workspace)
        snapshot = load_feature_lake_snapshot(self.workspace)

        self.assertEqual(
            len(snapshot.action_rows),
            features["action_feature_row_count"],
        )
        self.assertEqual(
            len(snapshot.history_rows),
            features["history_feature_row_count"],
        )
        self.assertEqual(
            len(snapshot.collaboration_rows),
            features["collaboration_feature_row_count"],
        )
        self.assertEqual(
            len(snapshot.static_rows),
            features["static_feature_row_count"],
        )
        self.assertEqual(
            len(snapshot.peer_group_rows),
            features["peer_group_feature_row_count"],
        )
        self.assertEqual(
            snapshot.state["action_manifest_key"],
            features["action_manifest_key"],
        )
        self.assertEqual(
            snapshot.state["history_manifest_key"],
            features["history_manifest_key"],
        )
        self.assertEqual(
            snapshot.state["collaboration_manifest_key"],
            features["collaboration_manifest_key"],
        )
        self.assertEqual(
            snapshot.state["static_manifest_key"],
            features["static_manifest_key"],
        )
        self.assertEqual(
            snapshot.state["peer_group_manifest_key"],
            features["peer_group_manifest_key"],
        )

    def test_feature_lake_feature_set_bridge_builds_python_feature_shape(self) -> None:
        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-feature-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                },
                {
                    "source": "okta",
                    "source_event_id": "okta-feature-0002",
                    "observed_at": "2026-02-11T10:05:00Z",
                    "received_at": "2026-02-11T10:06:00Z",
                    "actor_email": "bob@example.com",
                    "actor_name": "Bob Builder",
                    "department": "security",
                    "role": "manager",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-CA",
                    "ip": "10.0.0.2",
                    "privileged": True,
                },
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )
        run_local_processing_workers(self.workspace)

        feature_set = load_feature_lake_feature_set(self.workspace)

        self.assertEqual(feature_set.principal_vocab_size, 2)
        self.assertEqual(len(feature_set.actions), 1)
        self.assertEqual(len(feature_set.contexts), 2)
        self.assertTrue(self.workspace.load_feature_state()["scoring_input_ready"])
        self.assertEqual(self.workspace.load_feature_state()["scoring_input_mode"], "feature_lake")
        action = next(iter(feature_set.actions.values()))
        self.assertEqual(len(action.accessor_weights), 2)
        roles = {ctx.static.role for ctx in feature_set.contexts.values()}
        locations = {ctx.static.location for ctx in feature_set.contexts.values()}
        privileges = {ctx.static.privilege_level for ctx in feature_set.contexts.values()}
        peer_sets = [ctx.peers.department_peers for ctx in feature_set.contexts.values()]
        self.assertEqual(roles, {"security-admin", "manager"})
        self.assertEqual(locations, {"US-NY", "US-CA"})
        self.assertEqual(privileges, {"admin", "elevated"})
        self.assertEqual(peer_sets, [{1: 1.0}, {0: 1.0}])

    def test_ml_scoring_prefers_feature_lake_when_present(self) -> None:
        events = [
            {
                "source": "okta",
                "source_event_id": "okta-ml-0001",
                "observed_at": "2026-02-11T10:00:00Z",
                "received_at": "2026-02-11T10:01:00Z",
                "actor_email": "alice@example.com",
                "actor_name": "Alice Admin",
                "department": "security",
                "role": "security-admin",
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-NY",
                "ip": "10.0.0.1",
                "privileged": True,
            },
            {
                "source": "okta",
                "source_event_id": "okta-ml-0002",
                "observed_at": "2026-02-11T10:05:00Z",
                "received_at": "2026-02-11T10:06:00Z",
                "actor_email": "bob@example.com",
                "actor_name": "Bob Builder",
                "department": "security",
                "role": "manager",
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-CA",
                "ip": "10.0.0.2",
                "privileged": True,
            },
        ]
        submit_grouped_raw_events(
            self.workspace,
            records=events,
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )
        run_local_processing_workers(self.workspace)

        principals = [
            OrgPrincipal(
                idx=0,
                email="alice@example.com",
                name="Alice Admin",
                department="security",
                team="secops",
                role="security-admin",
                location="US-NY",
                is_manager=False,
                manager_idx=None,
                tz_offset=-5.0,
            ),
            OrgPrincipal(
                idx=1,
                email="bob@example.com",
                name="Bob Builder",
                department="security",
                team="secops",
                role="manager",
                location="US-CA",
                is_manager=True,
                manager_idx=None,
                tz_offset=-8.0,
            ),
        ]
        teams = [
            OrgTeam(
                name="secops",
                department="security",
                member_indices=[0, 1],
                manager_idx=1,
            )
        ]

        with patch(
            "seccloud.ml_scoring.build_features",
            side_effect=AssertionError("python feature pipeline should not run"),
        ):
            feature_set = _load_feature_set_for_scoring(
                events,
                principals,
                teams,
                workspace=self.workspace,
            )

        self.assertEqual(feature_set.principal_vocab_size, 2)
        self.assertEqual(len(feature_set.actions), 1)
        self.assertEqual(len(feature_set.contexts), 2)

    def test_ml_scoring_falls_back_when_feature_lake_is_partial(self) -> None:
        events = [
            {
                "source": "okta",
                "source_event_id": "okta-ml-partial-0001",
                "observed_at": "2026-02-11T10:00:00Z",
                "received_at": "2026-02-11T10:01:00Z",
                "actor_email": "alice@example.com",
                "actor_name": "Alice Admin",
                "department": "security",
                "role": "security-admin",
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-NY",
                "ip": "10.0.0.1",
                "privileged": True,
            },
            {
                "source": "okta",
                "source_event_id": "okta-ml-partial-0002",
                "observed_at": "2026-02-11T10:05:00Z",
                "received_at": "2026-02-11T10:06:00Z",
                "actor_email": "bob@example.com",
                "actor_name": "Bob Builder",
                "department": "security",
                "role": "manager",
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-CA",
                "ip": "10.0.0.2",
                "privileged": True,
            },
        ]
        submit_grouped_raw_events(
            self.workspace,
            records=events,
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )
        run_local_processing_workers(self.workspace)

        feature_state = self.workspace.load_feature_state()
        feature_state["static_manifest_key"] = None
        self.workspace.object_store.put_json("manifests/feature_state.json", feature_state)

        principals = [
            OrgPrincipal(
                idx=0,
                email="alice@example.com",
                name="Alice Admin",
                department="security",
                team="secops",
                role="security-admin",
                location="US-NY",
                is_manager=False,
                manager_idx=None,
                tz_offset=-5.0,
            ),
            OrgPrincipal(
                idx=1,
                email="bob@example.com",
                name="Bob Builder",
                department="security",
                team="secops",
                role="manager",
                location="US-CA",
                is_manager=True,
                manager_idx=None,
                tz_offset=-8.0,
            ),
        ]
        teams = [
            OrgTeam(
                name="secops",
                department="security",
                member_indices=[0, 1],
                manager_idx=1,
            )
        ]
        fallback_feature_set = object()

        with (
            patch(
                "seccloud.ml_scoring.load_feature_lake_feature_set",
                side_effect=AssertionError("lake should not load"),
            ),
            patch("seccloud.ml_scoring.build_features", return_value=fallback_feature_set) as build_features,
        ):
            feature_set = _load_feature_set_for_scoring(
                events,
                principals,
                teams,
                workspace=self.workspace,
            )

        self.assertIs(feature_set, fallback_feature_set)
        build_features.assert_called_once()
        self.assertFalse(self.workspace.load_feature_state()["scoring_input_ready"])
        self.assertEqual(
            self.workspace.load_feature_state()["scoring_input_mode"],
            "python_feature_pipeline_fallback",
        )

    def test_ml_scoring_falls_back_when_feature_lake_is_empty(self) -> None:
        events = [
            {
                "source": "okta",
                "source_event_id": "okta-ml-empty-0001",
                "observed_at": "2026-02-11T10:00:00Z",
                "received_at": "2026-02-11T10:01:00Z",
                "actor_email": "alice@example.com",
                "actor_name": "Alice Admin",
                "department": "security",
                "role": "security-admin",
                "event_type": "login",
                "resource_id": "okta:admin-console",
                "resource_name": "Admin Console",
                "resource_kind": "app",
                "sensitivity": "high",
                "geo": "US-NY",
                "ip": "10.0.0.1",
                "privileged": True,
            }
        ]
        principals = [
            OrgPrincipal(
                idx=0,
                email="alice@example.com",
                name="Alice Admin",
                department="security",
                team="secops",
                role="security-admin",
                location="US-NY",
                is_manager=False,
                manager_idx=None,
                tz_offset=-5.0,
            )
        ]
        teams = [
            OrgTeam(
                name="secops",
                department="security",
                member_indices=[0],
                manager_idx=None,
            )
        ]
        fallback_feature_set = object()

        with (
            patch(
                "seccloud.ml_scoring.load_feature_lake_feature_set",
                side_effect=AssertionError("lake should not load when empty"),
            ),
            patch("seccloud.ml_scoring.build_features", return_value=fallback_feature_set) as build_features,
        ):
            feature_set = _load_feature_set_for_scoring(
                events,
                principals,
                teams,
                workspace=self.workspace,
            )

        self.assertIs(feature_set, fallback_feature_set)
        build_features.assert_called_once()
        self.assertFalse(self.workspace.load_feature_state()["scoring_input_ready"])
        self.assertEqual(
            self.workspace.load_feature_state()["scoring_input_mode"],
            "python_feature_pipeline_fallback",
        )

    def test_feature_lake_bridge_uses_durable_peer_and_static_context_without_identity_profiles_sidecar(self) -> None:
        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-org-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                },
                {
                    "source": "okta",
                    "source_event_id": "okta-org-0002",
                    "observed_at": "2026-02-11T10:05:00Z",
                    "received_at": "2026-02-11T10:06:00Z",
                    "actor_email": "charlie@example.com",
                    "actor_name": "Charlie Check",
                    "department": "security",
                    "role": "engineer",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-IL",
                    "ip": "10.0.0.3",
                    "privileged": False,
                },
                {
                    "source": "okta",
                    "source_event_id": "okta-org-0003",
                    "observed_at": "2026-02-11T10:10:00Z",
                    "received_at": "2026-02-11T10:11:00Z",
                    "actor_email": "bob@example.com",
                    "actor_name": "Bob Builder",
                    "department": "security",
                    "role": "manager",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-CA",
                    "ip": "10.0.0.2",
                    "privileged": True,
                },
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )
        run_local_processing_workers(self.workspace)
        self.workspace.save_identity_profiles(
            {
                "manifest_version": 1,
                "source": "test-fixture",
                "generated_at": "2026-02-11T10:11:00Z",
                "principals": [
                    {
                        "idx": 0,
                        "email": "alice@example.com",
                        "name": "Alice Admin",
                        "department": "security",
                        "team": "secops",
                        "role": "security-admin",
                        "location": "US-NY",
                        "is_manager": False,
                        "manager_idx": 2,
                        "tz_offset": -5.0,
                    },
                    {
                        "idx": 1,
                        "email": "charlie@example.com",
                        "name": "Charlie Check",
                        "department": "security",
                        "team": "secops",
                        "role": "engineer",
                        "location": "US-IL",
                        "is_manager": False,
                        "manager_idx": 2,
                        "tz_offset": -6.0,
                    },
                    {
                        "idx": 2,
                        "email": "bob@example.com",
                        "name": "Bob Builder",
                        "department": "security",
                        "team": "secops",
                        "role": "manager",
                        "location": "US-CA",
                        "is_manager": True,
                        "manager_idx": None,
                        "tz_offset": -8.0,
                    },
                ],
                "teams": [
                    {
                        "name": "secops",
                        "department": "security",
                        "member_indices": [0, 1, 2],
                        "manager_idx": 2,
                    }
                ],
            },
        )
        features = run_feature_builder(self.workspace)
        self.workspace.identity_profiles_path.unlink()

        feature_set = load_feature_lake_feature_set(self.workspace)
        snapshot = load_feature_lake_snapshot(self.workspace)

        self.assertEqual(feature_set.principal_vocab_size, 3)
        self.assertGreaterEqual(features["peer_group_feature_row_count"], 9)
        self.assertEqual(len(snapshot.peer_group_rows), features["peer_group_feature_row_count"])
        manager_peer_sizes = sorted(len(ctx.peers.manager_peers) for ctx in feature_set.contexts.values())
        group_peer_sizes = sorted(len(ctx.peers.group_peers) for ctx in feature_set.contexts.values())
        duration_buckets = {ctx.static.employment_duration_bucket for ctx in feature_set.contexts.values()}
        self.assertEqual(manager_peer_sizes, [0, 1, 1])
        self.assertEqual(group_peer_sizes, [2, 2, 2])
        self.assertIn("<3mo", duration_buckets)
        self.assertIn("1-3yr", duration_buckets)

    def test_run_all_local_workers_includes_projection(self) -> None:
        submit_grouped_raw_events(
            self.workspace,
            records=[
                {
                    "source": "okta",
                    "source_event_id": "okta-all-workers-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "received_at": "2026-02-11T10:01:00Z",
                    "actor_email": "alice@example.com",
                    "actor_name": "Alice Admin",
                    "department": "security",
                    "role": "security-admin",
                    "event_type": "login",
                    "resource_id": "okta:admin-console",
                    "resource_name": "Admin Console",
                    "resource_kind": "app",
                    "sensitivity": "high",
                    "geo": "US-NY",
                    "ip": "10.0.0.1",
                    "privileged": True,
                }
            ],
            intake_kind="push_gateway",
            integration_id="test-gateway",
        )

        with patch(
            "seccloud.workers._run_rust_projector_worker",
            return_value={"dsn": "postgresql://projection", "event_count": 1, "detection_count": 0},
        ):
            result = run_all_local_workers(self.workspace, "postgresql://projection")

        self.assertIn("source_stats", result)
        self.assertIn("features", result)
        self.assertIn("detection_context", result)
        self.assertIn("projection", result)
        self.assertGreaterEqual(result["source_stats"]["source_count"], 1)
        self.assertEqual(result["projection"]["dsn"], "postgresql://projection")
        self.assertGreaterEqual(result["features"]["normalized_event_count"], 1)
        self.assertGreaterEqual(result["detection_context"]["event_count"], 1)

    def test_worker_service_once_requires_rust_runtime(self) -> None:
        with patch("seccloud.workers.shutil.which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "requires `cargo`"):
                run_worker_service_once(self.workspace, "postgresql://projection")

    def test_run_worker_service_once_uses_rust_runtime(self) -> None:
        rust_result = {
            "status": "processed",
            "pending_batch_count": 0,
            "processed_batch_count": 1,
            "result": {
                "normalization": {
                    "processed_batch_count": 1,
                    "processed_batch_ids": ["raw_1"],
                    "landed_record_count": 1,
                    "pending_batch_count": 0,
                    "ingest": {"added_normalized_events": 1},
                },
                "detect": {
                    "new_detection_count": 1,
                    "total_detection_count": 1,
                },
            },
        }

        with patch("seccloud.workers._run_rust_service_once", return_value=rust_result) as rust_service:
            result = run_worker_service_once(self.workspace, "postgresql://projection")

        self.assertEqual(result, rust_result)
        rust_service.assert_called_once_with(self.workspace, dsn="postgresql://projection", max_batches=None)

    def test_run_detection_worker_uses_rust_runtime(self) -> None:
        rust_result = {
            "detect": {
                "normalized_event_count": 4,
                "new_detection_count": 1,
                "total_detection_count": 2,
            },
            "ops_metadata": {
                "workspace": str(self.workspace.root),
                "event_counts_by_source": {"github": 4},
                "dead_letter_count": 0,
                "dead_letter_counts_by_source": {},
                "contains_raw_payloads": False,
            },
        }

        with patch("seccloud.workers._run_rust_detection_worker", return_value=rust_result) as rust_worker:
            result = run_detection_worker(self.workspace)

        self.assertEqual(result, rust_result)
        rust_worker.assert_called_once_with(self.workspace)

    def test_run_detection_mode_comparison_uses_rust_runtime(self) -> None:
        rust_result = {
            "event_count": 4,
            "scoring_runtime": {
                "effective_mode": "onnx_native",
            },
            "heuristic": {"detection_count": 1},
            "model": {"detection_count": 2},
            "overlap_detection_count": 1,
            "overlap_event_count": 1,
            "heuristic_only_detection_count": 0,
            "model_only_detection_count": 1,
            "heuristic_only_samples": [],
            "model_only_samples": [],
        }

        with patch(
            "seccloud.workers._run_rust_detection_mode_comparison",
            return_value=rust_result,
        ) as rust_compare:
            result = run_detection_mode_comparison(self.workspace)

        self.assertEqual(result, rust_result)
        rust_compare.assert_called_once_with(self.workspace)

    def test_run_detection_mode_evaluation_uses_rust_runtime(self) -> None:
        rust_result = {
            "event_count": 4,
            "heuristic": {"detection_count": 1},
            "model": {"detection_count": 2},
            "scoring_runtime": {"effective_mode": "onnx_native"},
        }

        with patch(
            "seccloud.workers._run_rust_detection_evaluation",
            return_value=rust_result,
        ) as rust_eval:
            result = run_detection_mode_evaluation(self.workspace)

        self.assertEqual(result, rust_result)
        rust_eval.assert_called_once_with(self.workspace)

    def test_run_detection_threshold_sweep_uses_rust_runtime(self) -> None:
        rust_result = {
            "event_count": 4,
            "source_sweeps": {
                "okta": {
                    "current_policy_threshold": 0.8,
                    "candidate_count": 6,
                    "recommended_threshold_for_5pct_budget": 0.9,
                    "recommended_attack_recall_for_5pct_budget": 0.4,
                    "points": [],
                }
            },
            "scoring_runtime": {"effective_mode": "onnx_native"},
        }

        with patch(
            "seccloud.workers._run_rust_detection_threshold_sweep",
            return_value=rust_result,
        ) as rust_sweep:
            result = run_detection_threshold_sweep(self.workspace)

        self.assertEqual(result, rust_result)
        rust_sweep.assert_called_once_with(self.workspace)

    def test_worker_service_loop_requires_rust_runtime(self) -> None:
        with patch("seccloud.workers.shutil.which", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "requires `cargo`"):
                run_worker_service_loop(
                    self.workspace,
                    dsn="postgresql://projection",
                    poll_interval_seconds=0,
                    exit_when_idle=True,
                )

    def test_run_worker_service_loop_uses_rust_runtime(self) -> None:
        rust_result = {
            "iterations": 1,
            "processed_iterations": 0,
            "idle_iterations": 1,
            "pending_batch_count": 0,
            "processed_batch_count": 0,
        }

        with patch("seccloud.workers._run_rust_service_loop", return_value=rust_result) as rust_loop:
            result = run_worker_service_loop(
                self.workspace,
                dsn="postgresql://projection",
                poll_interval_seconds=0,
                max_iterations=1,
                exit_when_idle=True,
            )

        self.assertEqual(result, rust_result)
        rust_loop.assert_called_once_with(
            self.workspace,
            dsn="postgresql://projection",
            poll_interval_seconds=0,
            max_batches=None,
            max_iterations=1,
            exit_when_idle=True,
        )

    def test_rust_runtime_command_uses_absolute_workspace_path(self) -> None:
        fake_workspace = SimpleNamespace(root=Path(".seccloud"), tenant_id="local")

        with patch(
            "seccloud.workers.subprocess.run",
            return_value=SimpleNamespace(stdout="{}", stderr="", returncode=0),
        ) as run_cmd:
            seccloud_workers._run_rust_runtime_command(fake_workspace, bin_name="seccloud-service-once")

        cmd = run_cmd.call_args.args[0]
        workspace_index = cmd.index("--workspace") + 1
        self.assertTrue(Path(cmd[workspace_index]).is_absolute())

    def test_rust_runtime_command_surfaces_stderr_on_failure(self) -> None:
        fake_workspace = SimpleNamespace(root=Path(".seccloud"), tenant_id="local")

        with patch(
            "seccloud.workers.subprocess.run",
            return_value=SimpleNamespace(stdout="", stderr="projection sync failed", returncode=101),
        ):
            with self.assertRaisesRegex(RuntimeError, "projection sync failed"):
                seccloud_workers._run_rust_runtime_command(fake_workspace, bin_name="seccloud-service-once")

    def test_operator_runtime_status_aggregates_runtime_state(self) -> None:
        initialize_runtime_stream(self.workspace)
        run_runtime(self.workspace)
        with patch(
            "seccloud.projection_store.fetch_projection_overview",
            return_value={"stream_state": {"cursor": 0}, "ops_metadata": {"dead_letter_count": 0}},
        ):
            status = build_operator_runtime_status(
                self.workspace,
                dsn="postgresql://projection",
                runtime_root=self.tempdir,
            )

        self.assertEqual(status["workspace"], str(self.workspace.root))
        self.assertEqual(status["tenant_id"], self.workspace.tenant_id)
        self.assertIn("worker_state", status)
        self.assertIn("feature_state", status)
        self.assertIn("feature_tables", status)
        self.assertIn("feature_vocab", status)
        self.assertIn("scoring_input", status)
        self.assertIn("model_runtime", status)
        self.assertIn("event_index", status)
        self.assertIn("detection_context", status)
        self.assertIn("identity_profiles", status)
        self.assertIn("stream_state", status)
        self.assertTrue(status["projection"]["available"])
        self.assertEqual(status["postgres"]["root"], str(self.tempdir.resolve()))
        self.assertGreaterEqual(status["feature_state"]["normalized_event_count"], 1)
        self.assertGreaterEqual(status["feature_tables"]["action_row_count"], 1)
        self.assertGreaterEqual(status["feature_tables"]["static_row_count"], 1)
        self.assertGreaterEqual(status["feature_tables"]["peer_group_row_count"], 0)
        self.assertGreaterEqual(status["event_index"]["event_count"], 1)
        self.assertGreaterEqual(status["event_index"]["principal_key_count"], 1)
        self.assertEqual(status["detection_context"]["event_count"], status["event_index"]["event_count"])
        self.assertTrue(status["scoring_input"]["ready"])
        self.assertEqual(status["scoring_input"]["mode"], "feature_lake")
        self.assertGreaterEqual(status["feature_vocab"]["principal_count"], 1)

    def test_bootstrap_local_runtime_starts_postgres_and_initializes_stream(self) -> None:
        with (
            patch(
                "seccloud.cli.start_local_postgres",
                return_value={"status": "started", "dsn": "postgresql://runtime"},
            ),
            patch(
                "seccloud.cli.initialize_runtime_stream",
                return_value={"status": "initialized", "mode": "continuous", "cursor": 0, "total_source_events": 0},
            ) as init_stream,
        ):
            result = bootstrap_local_runtime(self.workspace, runtime_root=self.tempdir)

        self.assertEqual(result["workspace"], str(self.workspace.root))
        self.assertEqual(result["postgres"]["status"], "started")
        self.assertEqual(result["stream"]["status"], "initialized")
        self.assertEqual(result["api_env"]["SECCLOUD_PROJECTION_DSN"], "postgresql://runtime")
        init_stream.assert_called_once_with(self.workspace, scaled=True, continuous=True)

    def test_scaled_runtime_stream_initializes_identity_profiles_without_precomputed_detection_manifest(self) -> None:
        dataset = SimpleNamespace(
            raw_events=[
                {
                    "source": "okta",
                    "source_event_id": "okta-stream-0001",
                    "observed_at": "2026-02-11T10:00:00Z",
                    "actor_email": "alice@example.com",
                    "scenario": "baseline",
                }
            ],
            expectations={},
        )
        principals = [
            OrgPrincipal(
                idx=0,
                email="alice@example.com",
                name="Alice Admin",
                department="security",
                team="secops",
                role="security-admin",
                location="US-NY",
                is_manager=False,
                manager_idx=None,
                tz_offset=-5.0,
            )
        ]
        teams = [OrgTeam(name="secops", department="security", member_indices=[0], manager_idx=None)]

        with (
            patch("seccloud.synthetic_scale.generate_scaled_dataset", return_value=dataset),
            patch("seccloud.synthetic_scale.generate_org", return_value=(principals, teams)),
        ):
            initialize_runtime_stream(
                self.workspace,
                scaled=True,
                num_principals=1,
                num_days=1,
                seed=7,
            )

        identity_profiles = json.loads(self.workspace.identity_profiles_path.read_text())
        self.assertEqual(len(identity_profiles["principals"]), 1)
        self.assertEqual(identity_profiles["principals"][0]["email"], "alice@example.com")
        self.assertEqual(identity_profiles["teams"][0]["name"], "secops")
        self.assertFalse((self.workspace.manifests_dir / "precomputed_detections.json").exists())

    def test_load_identity_profiles_ignores_runtime_org_compat_manifest(self) -> None:
        (self.workspace.manifests_dir / "runtime_org.json").write_text(
            json.dumps(
                {
                    "principals": [{"email": "legacy@example.com"}],
                    "teams": [{"name": "legacy-team"}],
                }
            ),
            encoding="utf-8",
        )

        empty_profiles = self.workspace.load_identity_profiles()

        self.assertEqual(empty_profiles["source"], "unknown")
        self.assertEqual(empty_profiles["principals"], [])
        self.assertEqual(empty_profiles["teams"], [])

        self.workspace.save_identity_profiles(
            {
                "manifest_version": 1,
                "source": "test-fixture",
                "generated_at": "2026-02-11T10:00:00Z",
                "principals": [{"email": "alice@example.com"}],
                "teams": [{"name": "secops"}],
            }
        )

        profiles = self.workspace.load_identity_profiles()

        self.assertEqual(profiles["source"], "test-fixture")
        self.assertEqual(profiles["principals"][0]["email"], "alice@example.com")
        self.assertEqual(profiles["teams"][0]["name"], "secops")

    def test_worker_service_once_projects_after_stream_reset_without_pending_batches(self) -> None:
        initialize_runtime_stream(self.workspace)

        with patch(
            "seccloud.workers._run_rust_service_once",
            return_value={
                "status": "projected",
                "pending_batch_count": 0,
                "processed_batch_count": 0,
                "result": {"projection": {"dsn": "postgresql://projection", "event_count": 0, "detection_count": 0}},
            },
        ):
            result = run_worker_service_once(self.workspace, "postgresql://projection")

        self.assertEqual(result["status"], "projected")

    def test_worker_service_once_materializes_source_stats_without_pending_batches(self) -> None:
        seed_workspace(self.workspace)
        ingest_raw_events(self.workspace)
        self.workspace.save_source_stats({"sources": {}})
        self.workspace.request_source_stats_refresh()

        with patch(
            "seccloud.workers._run_rust_service_once",
            return_value={
                "status": "materialized",
                "pending_batch_count": 0,
                "processed_batch_count": 1,
                "result": {"source_stats": {"source_count": 1}},
            },
        ):
            result = run_worker_service_once(self.workspace, "postgresql://projection")

        self.assertEqual(result["status"], "materialized")
        self.assertIn("source_stats", result["result"])


if __name__ == "__main__":
    unittest.main()
