#!/usr/bin/env python3
"""
ck_pipeline.py

Lab02S01 helper script:
- clones one Java repository
- organizes local folders
- runs CK on the cloned repository
- handles errors/logging
- summarizes quality metrics
- exports a first CSV with quality metrics for 1 repository

Expected external dependencies:
- git installed and available in PATH
- java installed and available in PATH
- CK jar file downloaded locally

Example:
    python ck_pipeline.py \
        --repo-url https://github.com/google/gson.git \
        --ck-jar ck/ck-0.7.1-SNAPSHOT-jar-with-dependencies.jar  \
        --workspace ./workspace
"""

from __future__ import annotations

import argparse
import csv
import logging
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Dict, Iterable, List, Optional


# -----------------------------
# Data models
# -----------------------------

@dataclass
class PipelinePaths:
    workspace: Path
    repos_dir: Path
    raw_ck_dir: Path
    summaries_dir: Path
    logs_dir: Path

    @classmethod
    def from_workspace(cls, workspace: Path) -> "PipelinePaths":
        return cls(
            workspace=workspace,
            repos_dir=workspace / "repos",
            raw_ck_dir=workspace / "ck_raw",
            summaries_dir=workspace / "summaries",
            logs_dir=workspace / "logs",
        )


@dataclass
class RepoExecutionResult:
    repo_url: str
    repo_name: str
    repo_path: Path
    ck_output_dir: Path
    summary_csv: Path
    status: str
    error_message: Optional[str] = None


# -----------------------------
# Logging
# -----------------------------

def setup_logging(log_file: Path, verbose: bool = False) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO
    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True,
    )


# -----------------------------
# Utilities
# -----------------------------

def run_command(
    command: List[str],
    *,
    cwd: Optional[Path] = None,
    timeout: int = 3600,
) -> subprocess.CompletedProcess:
    logging.debug("Running command: %s", " ".join(command))
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def ensure_workspace(paths: PipelinePaths) -> None:
    for path in [
        paths.workspace,
        paths.repos_dir,
        paths.raw_ck_dir,
        paths.summaries_dir,
        paths.logs_dir,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def derive_repo_name(repo_url: str) -> str:
    name = repo_url.rstrip("/").split("/")[-1]
    if name.endswith(".git"):
        name = name[:-4]
    if not name:
        raise ValueError(f"Could not derive repository name from URL: {repo_url}")
    return name


def is_probably_git_repo_url(repo_url: str) -> bool:
    return repo_url.startswith("https://github.com/") or repo_url.startswith("git@github.com:")


def check_binary_available(binary_name: str) -> None:
    if shutil.which(binary_name) is None:
        raise EnvironmentError(f"Required binary not found in PATH: {binary_name}")


# -----------------------------
# Clone automation
# -----------------------------

def clone_repository(repo_url: str, destination: Path, force_reclone: bool = False) -> Path:
    if not is_probably_git_repo_url(repo_url):
        raise ValueError(f"Invalid or unsupported repository URL: {repo_url}")

    repo_name = derive_repo_name(repo_url)
    repo_path = destination / repo_name

    if repo_path.exists():
        if force_reclone:
            logging.info("Removing existing repository folder: %s", repo_path)
            shutil.rmtree(repo_path)
        else:
            logging.info("Repository already exists. Reusing local clone: %s", repo_path)
            return repo_path

    logging.info("Cloning repository: %s", repo_url)
    result = run_command(["git", "clone", "--depth", "1", repo_url, str(repo_path)])

    if result.returncode != 0:
        logging.error("Clone failed for %s", repo_url)
        logging.debug("git stdout: %s", result.stdout)
        logging.debug("git stderr: %s", result.stderr)
        raise RuntimeError(f"Clone failed: {result.stderr.strip() or result.stdout.strip()}")

    return repo_path


# -----------------------------
# CK execution pipeline
# -----------------------------

def run_ck(ck_jar: Path, repo_path: Path, output_dir: Path, timeout: int = 3600) -> None:
    if not ck_jar.exists():
        raise FileNotFoundError(f"CK jar not found: {ck_jar}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Common CK invocation pattern:
    # java -jar ck.jar <project_path> false 0 false <output_dir>
    command = [
        "java",
        "-jar",
        str(ck_jar),
        str(repo_path),
        "false",
        "0",
        "false",
        str(output_dir),
    ]

    logging.info("Running CK for repository: %s", repo_path.name)
    result = run_command(command, timeout=timeout)

    if result.returncode != 0:
        logging.error("CK execution failed for %s", repo_path)
        logging.debug("CK stdout: %s", result.stdout)
        logging.debug("CK stderr: %s", result.stderr)
        raise RuntimeError(f"CK execution failed: {result.stderr.strip() or result.stdout.strip()}")

    logging.info("CK finished successfully for %s", repo_path.name)


# -----------------------------
# CSV parsing and summarization
# -----------------------------

def _safe_float(value: str) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pick_first_existing_column(fieldnames: Iterable[str], candidates: List[str]) -> Optional[str]:
    normalized = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    return None


def read_class_metrics(class_csv_path: Path) -> Dict[str, List[float]]:
    """
    Reads CK class.csv and extracts values for:
    - CBO
    - DIT
    - LCOM

    CK versions may use slightly different column names for LCOM,
    so this function tries common alternatives.
    """
    if not class_csv_path.exists():
        raise FileNotFoundError(f"class.csv not found: {class_csv_path}")

    metrics: Dict[str, List[float]] = {
        "cbo": [],
        "dit": [],
        "lcom": [],
    }

    with class_csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"class.csv has no header: {class_csv_path}")

        cbo_col = _pick_first_existing_column(reader.fieldnames, ["cbo"])
        dit_col = _pick_first_existing_column(reader.fieldnames, ["dit"])
        lcom_col = _pick_first_existing_column(reader.fieldnames, ["lcom", "lcom*", "lcom_hs", "lcomhs"])

        if cbo_col is None or dit_col is None or lcom_col is None:
            raise ValueError(
                "Could not find required CK columns in class.csv. "
                f"Found columns: {reader.fieldnames}"
            )

        for row in reader:
            cbo = _safe_float(row.get(cbo_col, ""))
            dit = _safe_float(row.get(dit_col, ""))
            lcom = _safe_float(row.get(lcom_col, ""))

            if cbo is not None:
                metrics["cbo"].append(cbo)
            if dit is not None:
                metrics["dit"].append(dit)
            if lcom is not None:
                metrics["lcom"].append(lcom)

    return metrics


def summarize_metric(values: List[float]) -> Dict[str, float]:
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "median": 0.0,
            "stddev": 0.0,
            "min": 0.0,
            "max": 0.0,
        }

    return {
        "count": len(values),
        "mean": mean(values),
        "median": median(values),
        "stddev": pstdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
    }


def write_summary_csv(
    summary_csv_path: Path,
    repo_name: str,
    repo_url: str,
    metrics: Dict[str, List[float]],
) -> None:
    summary_csv_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for metric_name in ["cbo", "dit", "lcom"]:
        stats = summarize_metric(metrics.get(metric_name, []))
        rows.append(
            {
                "repo_name": repo_name,
                "repo_url": repo_url,
                "metric": metric_name.upper(),
                "count": stats["count"],
                "mean": round(stats["mean"], 4),
                "median": round(stats["median"], 4),
                "stddev": round(stats["stddev"], 4),
                "min": round(stats["min"], 4),
                "max": round(stats["max"], 4),
            }
        )

    with summary_csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["repo_name", "repo_url", "metric", "count", "mean", "median", "stddev", "min", "max"],
        )
        writer.writeheader()
        writer.writerows(rows)


# -----------------------------
# End-to-end pipeline for 1 repository
# -----------------------------

def execute_one_repository_pipeline(
    repo_url: str,
    ck_jar: Path,
    paths: PipelinePaths,
    force_reclone: bool = False,
    ck_timeout: int = 3600,
) -> RepoExecutionResult:
    repo_name = derive_repo_name(repo_url)
    repo_path = paths.repos_dir / repo_name
    ck_output_dir = paths.raw_ck_dir / repo_name
    summary_csv = paths.summaries_dir / f"{repo_name}_quality_summary.csv"

    try:
        logging.info("Starting pipeline for repository: %s", repo_url)

        cloned_repo_path = clone_repository(
            repo_url=repo_url,
            destination=paths.repos_dir,
            force_reclone=force_reclone,
        )

        run_ck(
            ck_jar=ck_jar,
            repo_path=cloned_repo_path,
            output_dir=ck_output_dir,
            timeout=ck_timeout,
        )

        class_csv_path = ck_output_dir / "class.csv"
        metrics = read_class_metrics(class_csv_path)

        write_summary_csv(
            summary_csv_path=summary_csv,
            repo_name=repo_name,
            repo_url=repo_url,
            metrics=metrics,
        )

        logging.info("Pipeline completed successfully for %s", repo_name)
        logging.info("Summary CSV generated at: %s", summary_csv)

        return RepoExecutionResult(
            repo_url=repo_url,
            repo_name=repo_name,
            repo_path=repo_path,
            ck_output_dir=ck_output_dir,
            summary_csv=summary_csv,
            status="success",
        )

    except Exception as exc:
        logging.exception("Pipeline failed for repository: %s", repo_url)
        return RepoExecutionResult(
            repo_url=repo_url,
            repo_name=repo_name,
            repo_path=repo_path,
            ck_output_dir=ck_output_dir,
            summary_csv=summary_csv,
            status="failed",
            error_message=str(exc),
        )


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clone a Java repository, run CK, and export a first quality metrics CSV."
    )
    parser.add_argument(
        "--repo-url",
        required=True,
        help="GitHub repository URL, e.g. https://github.com/google/gson.git",
    )
    parser.add_argument(
        "--ck-jar",
        required=True,
        type=Path,
        help="Path to CK jar file.",
    )
    parser.add_argument(
        "--workspace",
        required=False,
        type=Path,
        default=Path("./lab02_workspace"),
        help="Base workspace folder.",
    )
    parser.add_argument(
        "--force-reclone",
        action="store_true",
        help="Delete local repository folder and clone again.",
    )
    parser.add_argument(
        "--ck-timeout",
        type=int,
        default=3600,
        help="Timeout in seconds for CK execution.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        check_binary_available("git")
        check_binary_available("java")
    except Exception as exc:
        print(f"[ERROR] Environment validation failed: {exc}", file=sys.stderr)
        return 2

    paths = PipelinePaths.from_workspace(args.workspace)
    ensure_workspace(paths)

    log_file = paths.logs_dir / "lab02_ck_pipeline.log"
    setup_logging(log_file=log_file, verbose=args.verbose)

    result = execute_one_repository_pipeline(
        repo_url=args.repo_url,
        ck_jar=args.ck_jar,
        paths=paths,
        force_reclone=args.force_reclone,
        ck_timeout=args.ck_timeout,
    )

    if result.status == "success":
        print("\nPipeline finished successfully.")
        print(f"Repository:   {result.repo_name}")
        print(f"Local clone:  {result.repo_path}")
        print(f"CK output:    {result.ck_output_dir}")
        print(f"Summary CSV:  {result.summary_csv}")
        print(f"Log file:     {log_file}")
        return 0

    print("\nPipeline failed.")
    print(f"Repository:   {result.repo_name}")
    print(f"Error:        {result.error_message}")
    print(f"Log file:     {log_file}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())