#!/usr/bin/env python3

import argparse
import datetime
import glob
import os
import sys

import dask
from dask.distributed import Client, as_completed

from LiLF import lib_cfg, lib_scheduler, lib_log, lib_walker
logger_obj = lib_log.Logger('PiLL')
logger = lib_log.logger
s = lib_scheduler.Scheduler(dry_run=False)
w = lib_walker.Walker('PiLL.walker')

from LiLF.pipelines import download, setup, calibrator, timesplit, combine, ddparallel, ddserial, quality, extract, facetselfcal

def check_done(step: lib_cfg.Step):
    """Verify that the step's log file ends with a 'Done' line.

    Every pipeline run() calls w.alldone() as its last action, which writes
    'Done. Total time: ...' to the log.  If that line is absent the step
    either crashed silently or was interrupted before completing.

    Raises RuntimeError so the caller can decide how to handle it (the walker
    will not mark the step as done and it will be retried on the next run).
    """
    pattern = f'pipeline-{step.kind}-{step.name}_*.logger'
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise RuntimeError(
            f"No log file found for step '{step.name}' ({step.kind}) — "
            f"pattern searched: {pattern!r}"
        )
    logfile = matches[-1]   # most recent run
    with open(logfile, 'r') as f:
        lines = [l for l in f.readlines() if l.strip()]
    last = lines[-1].strip() if lines else ''
    if 'Done' not in last:
        raise RuntimeError(
            f"Step '{step.name}' ({step.kind}) did not finish cleanly.  "
            f"Last log line: {last!r}"
        )


def run_step(step: lib_cfg.Step):
    """
    Dispatch a single step to its handler based on kind.
    Replace each handler call with your actual implementation.
    """
    handlers = {
        'download':     download.run,
        'setup':        setup.run,
        'calibrator':   calibrator.run,
        'timesplit':    timesplit.run,
        'combine':      combine.run,
        'ddparallel':   ddparallel.run,
        'ddserial':     ddserial.run,
        'quality':      quality.run,
        'extract':      extract.run,
        'facetselfcal': facetselfcal.run,
    }
    handler = handlers.get(step.kind)
    if handler is None:
        raise ValueError(f"No handler registered for kind '{step.kind}'")
    handler(step)

@dask.delayed
def execute(step, *parent_steps):
    timeinit = datetime.datetime.now()
    run_step(step)
    check_done(step)
    w.mark_done(step.name, timeinit=timeinit)
    logger.info(f'  done: {step.name}')
    return step.name

def run_pipeline(pipeline: lib_cfg.Pipeline, dry_run: bool = False, max_workers: int = 1):
    """
    Execute the pipeline respecting serial/parallel structure.

    Each top-level stage in execution_plan() is either:
        Step          -> run alone, block until done
        list[Step]    -> run all in parallel, block until ALL done

    Args:
        pipeline:    parsed Pipeline object
        dry_run:     if True, print what would run without executing
        max_workers: max parallel workers for parallel stages
    """
    client = Client(
        n_workers=max_workers,
        threads_per_worker=1,
    )

    print("dashboard_link", client.dashboard_link)

    plan = pipeline.execution_plan()

    # Validate before running anything
    errors = pipeline.validate()
    if errors:
        logger.error('Pipeline validation failed — aborting.')
        for e in errors:
            logger.error(f'  [{e.step_name}] ({e.kind}) missing: {e.missing}')
        sys.exit(1)

    dask_tasks = []
    for i, stage in enumerate(plan, 1):

        # --- parallel stage ---
        if isinstance(stage, list):
            names = [s.name for s in stage]
            logger.info(f'[{i:02d}] PARALLEL: {names} (workers: {max_workers})')

            if dry_run:
                for s in stage:
                    logger.info(f'  would run: {s.name} ({s.kind})')
                continue

            parent_step = None
            for step in stage:
                # We use is_done() directly rather than the if_todo() context manager
                # because submission and completion happen at different points in time —
                # the context manager would mark the step done as soon as the 'with'
                # block exits (i.e. right after submit()), before we know the outcome.
                if w.is_done(step.name):
                    logger.warning(f'>> skip << {step.name}')
                    continue
                logger.info(f'  submitting: {step.name} ({step.kind})')
                parent_step = execute(step, parent_step)
            dask_tasks.append(parent_step)

        # --- serial stage ---
        else:
            if dry_run:
                logger.info(f'[{i:02d}] SERIAL:   {stage.name} ({stage.kind}) — dry run')
                continue

            with w.if_todo(stage.name):
                logger.info(f'[{i:02d}] SERIAL:   {stage.name} ({stage.kind})')
                pill_handlers = logger.handlers[:]   # save PiLL's handlers before the step

                parent_step = execute(stage, *dask_tasks)
                dask_tasks = [parent_step]

    dask.compute(dask_tasks)

if __name__ == "__main__":
    # Accept an explicit config file path as the sole optional positional argument;
    # fall back to auto-discovery in the current directory when none is given.
    parser = argparse.ArgumentParser(
        description='PiLL: Pipeline for LOFAR LBA - runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        'config',
        nargs='?',
        default=None,
        metavar='CONFIG_FILE',
        help='Path to the LiLF config/parset file (optional). '
            'When omitted, the pipeline searches the current directory for '
            'a file matching LiLF.conf, LiLF.config, LiLF.cfg, or LiLF.parset.',
    )
    args = parser.parse_args()

    parsetFile = args.config

    pipeline = lib_cfg.read_parset(args.config)
    logger.info(pipeline.describe(verbose=True))

    # Set up the working directory and run the pipeline.
    max_workers = pipeline.get('max_workers')
    dry_run = pipeline.get('dry_run')
    working_dir = os.path.abspath(pipeline.get('working_dir'))
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)
    os.chdir(working_dir)

    run_pipeline(pipeline, dry_run=dry_run, max_workers=max_workers)

    logger.info('### %s: Done. #####################################')
