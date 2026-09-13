#!/usr/bin/env python3
"""What a production does when the CRAB server refuses a submission, or has not scheduled it yet.

On 2026-09-12 a 36000-branch Run3_2022EE production died. The CRAB server had refused its task --
`Status on the CRAB server: SUBMITREFUSED`, `Warning: A site name T3_CH_CERN_HelixNebula_REHA that
user specified is not in the list of known CMS Processing Site Names` -- because DSProd expanded
its `T3_*` whitelist glob against a site list built by the wrong rule. A refused task never
produces per-job information, law reports that as an unreadable status, and DSProd kept retrying it
as if it were slow: 4 attempts per poll with 15 s between them, for 16 consecutive polls, ~1050
repetitions, until the driver died.

Two things are pinned here. A task the server has *refused* is terminal, so its jobs are failed and
law submits them again as a new task. A task the server has merely not *scheduled* yet is normal,
so it costs neither the retry delay nor a step towards `max_unreadable_polls` -- law 0.1.20 does not
know the `WAITING` status that every task now starts in, which is why a healthy submission looked
unreadable for its first polls.
"""

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

dsprod_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dsprod_repo not in sys.path:
    sys.path.insert(0, dsprod_repo)

os.environ["ANALYSIS_PATH"] = dsprod_repo

import law  # noqa: E402

law.contrib.load("cms")

from dsprod.crab import (  # noqa: E402
    CrabTaskNotScheduledYet,
    CrabTaskRefused,
    CrabWorkflow,
    DSProdCrabJobManager,
    _parse_cric_sites,
    processing_sites,
)

SANDBOX = "cmssw::CMSSW_14_0_0::arch=el9_amd64_gcc12"

#: the response of the incident, trimmed to the lines that decide the outcome
REFUSED_OUTPUT = """Rucio client intialized for account kandroso
CRAB project directory:\t\t/eos/.../crab_RunProd_Run3_XHHbbWW_a741224d
Task name:\t\t\t260912_054021:kandroso_crab_RunProd_Run3_XHHbbWW_a741224d
Grid scheduler - Task Worker:\tN/A yet - crab-prod-tw01
Status on the CRAB server:\tSUBMITREFUSED
Warning:\t\tA site name T3_CH_CERN_HelixNebula_REHA that user specified is not in the list of known CMS Processing Site Names
Log file is /eos/.../crab.log
"""

WAITING_OUTPUT = REFUSED_OUTPUT.replace(
    "Status on the CRAB server:\tSUBMITREFUSED",
    "Status on the CRAB server:\tWAITING on command SUBMIT",
)

SUBMITTED_OUTPUT = REFUSED_OUTPUT.replace(
    "Status on the CRAB server:\tSUBMITREFUSED",
    "Status on the CRAB server:\tSUBMITTED",
)

#: what law genuinely cannot read: no server status at all. A `SUBMITTED` response without job
#: data is NOT this case -- law accepts that status itself and reports the jobs pending.
UNREADABLE_OUTPUT = (
    "Rucio client intialized for account kandroso\nSome unexpected output\n"
)


def manager(**attrs):
    m = DSProdCrabJobManager(sandbox_name=SANDBOX)
    for key, value in attrs.items():
        setattr(m, key, value)
    return m


def job_ids(m, n=3):
    return [m.JobId(i, "260912_054021:task", "/proj") for i in range(1, n + 1)]


class ReadingTheServerStatus(unittest.TestCase):
    """law's regex is anchored and applied per line; the obvious way to use it finds nothing."""

    def test_the_status_is_found_the_way_law_finds_it(self):
        self.assertEqual(
            DSProdCrabJobManager.server_status(REFUSED_OUTPUT), "SUBMITREFUSED"
        )
        self.assertEqual(
            DSProdCrabJobManager.server_state(REFUSED_OUTPUT), "SUBMITREFUSED"
        )

    def test_searching_the_whole_response_would_have_found_nothing(self):
        """The trap this pins: `query_server_status_cre` carries no `re.MULTILINE`."""
        self.assertIsNone(
            DSProdCrabJobManager.query_server_status_cre.search(REFUSED_OUTPUT)
        )

    def test_the_command_half_is_dropped(self):
        self.assertEqual(DSProdCrabJobManager.server_state(WAITING_OUTPUT), "WAITING")

    def test_the_reason_comes_from_the_warning_line(self):
        """A refused task sets no `Failure message from server`; the reason is a task warning."""
        warnings = DSProdCrabJobManager.server_warnings(REFUSED_OUTPUT)
        self.assertEqual(len(warnings), 1)
        self.assertIn("T3_CH_CERN_HelixNebula_REHA", warnings[0])

    def test_an_empty_response_says_nothing_rather_than_raising(self):
        self.assertIsNone(DSProdCrabJobManager.server_status(""))
        self.assertEqual(DSProdCrabJobManager.server_state(None), "")


class TellingApartTheThreeUnreadableResponses(unittest.TestCase):
    """All three reach law's "no per-job information" error; only one is terminal."""

    def parse(self, out):
        return DSProdCrabJobManager.parse_query_output(out, "/proj", [1, 2, 3])

    def test_a_refused_task_is_raised_as_refused(self):
        with self.assertRaises(CrabTaskRefused) as caught:
            self.parse(REFUSED_OUTPUT)
        self.assertEqual(caught.exception.state, "SUBMITREFUSED")
        self.assertIn("T3_CH_CERN_HelixNebula_REHA", str(caught.exception))

    def test_a_waiting_task_is_raised_as_not_scheduled_yet(self):
        with self.assertRaises(CrabTaskNotScheduledYet) as caught:
            self.parse(WAITING_OUTPUT)
        self.assertEqual(caught.exception.state, "WAITING")

    def test_anything_else_keeps_the_old_diagnostic(self):
        with self.assertRaises(Exception) as caught:
            self.parse(UNREADABLE_OUTPUT)
        for kind in (CrabTaskRefused, CrabTaskNotScheduledYet):
            self.assertNotIsInstance(caught.exception, kind)
        self.assertIn("first lines of what crab returned", str(caught.exception))

    def test_a_task_that_still_reports_its_jobs_is_never_touched(self):
        """The expensive mistake: blanket-failing a task whose real per-job states are right there.

        The check runs only after law has refused the response, so a response law *can* parse --
        whatever the server status says -- is returned unchanged.
        """
        parsed = {"1": "real per-job states"}
        with mock.patch.object(
            law.cms.CrabJobManager, "parse_query_output", return_value=parsed
        ):
            self.assertEqual(self.parse(REFUSED_OUTPUT), parsed)


class WhenTheServerRefuses(unittest.TestCase):
    def query(self, m, out=REFUSED_OUTPUT, n=3, proj_dir="/proj", ours=True):
        if ours:
            m._submitted_projects.add(proj_dir)
        ids = job_ids(m, n)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            side_effect=lambda *a, **k: DSProdCrabJobManager.parse_query_output(
                out, proj_dir, ids
            ),
        ), mock.patch("dsprod.crab.time.sleep") as slept:
            return m.query(proj_dir, job_ids=ids), ids, slept

    def test_every_job_is_reported_failed_so_law_submits_them_again(self):
        m = manager()
        result, ids, _ = self.query(m)
        self.assertEqual(set(result), set(ids))
        for data in result.values():
            self.assertEqual(data["status"], m.FAILED)

    def test_no_site_is_blamed_for_a_task_that_never_ran(self):
        """`harvest_site_stats` charges a site only for a failure with a job-level code."""
        m = manager()
        result, _, _ = self.query(m)
        self.assertTrue(all(data["code"] is None for data in result.values()))

    def test_the_error_is_not_the_one_the_mass_retry_brake_counts(self):
        m = manager()
        result, _, _ = self.query(m)
        for data in result.values():
            self.assertNotIn("initially missing task outputs", str(data["error"]))
            self.assertIn("SUBMITREFUSED", str(data["error"]))

    def test_nothing_waits_for_a_verdict_that_cannot_change(self):
        m = manager()
        _, _, slept = self.query(m)
        slept.assert_not_called()

    def test_it_does_not_count_towards_the_unreadable_ceiling(self):
        m = manager()
        m._unreadable["/proj"] = 7
        self.query(m)
        self.assertNotIn("/proj", m._unreadable)

    def test_the_reason_is_reported_once_per_task_not_once_per_poll(self):
        m = manager(max_refused_submissions=99)
        with mock.patch("builtins.print") as printed:
            self.query(m)
            self.query(m)
        said = "\n".join(str(c.args[0]) for c in printed.call_args_list if c.args)
        self.assertEqual(said.count("T3_CH_CERN_HelixNebula_REHA"), 1)
        self.assertIn("preset=site-names", said)

    def test_the_cached_site_list_is_dropped_so_the_next_wave_re_reads_cric(self):
        with tempfile.TemporaryDirectory() as d:
            cache = os.path.join(d, "cms_psn_sites.json")
            with open(cache, "w") as f:
                json.dump(["T3_CH_CERN_HelixNebula_REHA"], f)
            m = manager(site_cache_path=cache)
            self.query(m)
            self.assertFalse(os.path.exists(cache))

    def test_one_refusal_alone_does_not_stop_the_run(self):
        """It can be a stale site list, which is dropped here, so the next wave differs."""
        m = manager()
        self.query(m)
        self.assertIsNone(m.stop_reason)

    def test_a_second_refused_submission_records_a_stop_reason_that_says_why(self):
        m = manager()
        self.query(m, proj_dir="/proj-1")
        self.query(m, proj_dir="/proj-2")
        self.assertIn("refused", m.stop_reason)
        self.assertIn("T3_CH_CERN_HelixNebula_REHA", m.stop_reason)

    def test_polling_one_refused_task_again_is_one_refusal_not_two(self):
        """The counter is per submission; law re-queries a project every poll."""
        m = manager()
        for _ in range(5):
            self.query(m, proj_dir="/proj-1")
        self.assertIsNone(m.stop_reason)

    def test_the_jobs_are_still_failed_on_the_poll_that_stops_the_run(self):
        """law must not be left with a task whose jobs were never marked, whichever way it ends."""
        m = manager()
        self.query(m, proj_dir="/proj-1")
        result, ids, _ = self.query(m, proj_dir="/proj-2")
        self.assertEqual(set(result), set(ids))
        self.assertTrue(all(d["status"] == m.FAILED for d in result.values()))
        self.assertIsNotNone(m.stop_reason)


class WhenTheServerHasNotScheduledItYet(unittest.TestCase):
    """Every task now starts in WAITING, which law 0.1.20 does not accept."""

    def query(self, m):
        ids = job_ids(m)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            side_effect=lambda *a, **k: DSProdCrabJobManager.parse_query_output(
                WAITING_OUTPUT, "/proj", ids
            ),
        ), mock.patch("dsprod.crab.time.sleep") as slept:
            return m.query("/proj", job_ids=ids), ids, slept

    def test_its_jobs_are_pending_not_failed(self):
        m = manager()
        result, ids, _ = self.query(m)
        self.assertEqual(set(result), set(ids))
        for data in result.values():
            self.assertEqual(data["status"], m.PENDING)

    def test_it_costs_neither_a_retry_delay_nor_a_step_towards_the_ceiling(self):
        m = manager()
        _, _, slept = self.query(m)
        slept.assert_not_called()
        self.assertNotIn("/proj", m._unreadable)

    def test_but_it_is_still_reported_once(self):
        m = manager()
        with mock.patch("builtins.print") as printed:
            self.query(m)
            self.query(m)
        said = [str(c.args[0]) for c in printed.call_args_list if c.args]
        self.assertEqual(len([s for s in said if "WAITING" in s]), 1)


class ASlowTaskIsStillTreatedAsSlow(unittest.TestCase):
    """The behaviour that must not regress: an unreadable response of any other kind."""

    def test_an_unreadable_response_keeps_the_old_path(self):
        m = manager()
        ids = job_ids(m)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            side_effect=lambda *a, **k: DSProdCrabJobManager.parse_query_output(
                UNREADABLE_OUTPUT, "/proj", ids
            ),
        ), mock.patch("dsprod.crab.time.sleep") as slept:
            result = m.query("/proj", job_ids=ids)
        for data in result.values():
            self.assertEqual(data["status"], m.PENDING)
        self.assertEqual(m._unreadable["/proj"], 1)
        self.assertEqual(slept.call_count, m.query_retries)


class StoppingTheRunActuallyStopsIt(unittest.TestCase):
    """Where the stop is raised decides whether it stops anything at all.

    law queries through a thread pool and `get_async_result_silent` turns an exception there into
    the *result*, so raising from `query` would be counted as one more unreadable poll: the refused
    task's jobs would stay unfailed and every other project would lose that poll's status. The poll
    callback is the one hook the loop calls outside its own error handling.
    """

    def test_law_would_swallow_a_raise_from_query(self):
        """The premise, pinned against the installed law rather than assumed."""
        from law.job.base import get_async_result_silent

        class Boom:
            def get(self, timeout=None):
                raise RuntimeError("stop the run")

        self.assertIsInstance(get_async_result_silent(Boom()), RuntimeError)

    def test_the_poll_callback_raises_what_the_manager_recorded(self):
        workflow = mock.Mock(spec=CrabWorkflow)
        workflow._dsprod_job_manager = manager(stop_reason="two refused submissions")
        with self.assertRaises(RuntimeError) as caught:
            CrabWorkflow.crab_poll_callback(workflow, mock.Mock())
        self.assertIn("two refused submissions", str(caught.exception))

    def test_the_manager_that_polls_is_the_one_the_callback_reads(self):
        """The single link in the stop chain: without this assignment nothing is ever raised."""
        # law refuses to instantiate an abstract workflow, so a concrete stand-in; never
        # __init__'d, because constructing one wants a scheduler
        concrete = type(
            "ConcreteCrabWorkflow",
            (CrabWorkflow,),
            {"create_branch_map": lambda self: {}, "run": lambda self: None},
        )
        workflow = concrete.__new__(concrete)
        built = manager()
        with mock.patch.object(
            law.cms.CrabWorkflow, "crab_create_job_manager", return_value=built
        ), mock.patch.object(
            CrabWorkflow, "site_stats", return_value=None
        ), mock.patch.object(
            CrabWorkflow, "job_watchdog", return_value=None
        ), mock.patch.object(
            CrabWorkflow, "site_cache_path", return_value="/data/cms_psn_sites.json"
        ), mock.patch.object(
            type(built), "cmssw_env", new_callable=mock.PropertyMock, return_value={}
        ):
            returned = workflow.crab_create_job_manager()
        self.assertIs(returned, built)
        self.assertIs(workflow._dsprod_job_manager, built)

    def test_it_does_nothing_when_there_is_nothing_to_report(self):
        workflow = mock.Mock(spec=CrabWorkflow)
        workflow._dsprod_job_manager = manager()
        workflow._crab_kerberos_update = lambda: None
        CrabWorkflow.crab_poll_callback(workflow, mock.Mock())


class ARefusalInheritedFromAnEarlierRun(unittest.TestCase):
    """The 2026-09-13 restart: three tasks refused *before* the fix was deployed were re-polled by
    the corrected run, counted as its own refusals, and stopped it on the first poll -- before the
    5000 branches they held could be submitted again with the corrected whitelist."""

    def query(self, m, proj_dir):
        ids = job_ids(m)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            side_effect=lambda *a, **k: DSProdCrabJobManager.parse_query_output(
                REFUSED_OUTPUT, proj_dir, ids
            ),
        ), mock.patch("dsprod.crab.time.sleep"):
            return m.query(proj_dir, job_ids=ids), ids

    def test_old_refusals_do_not_stop_a_corrected_run(self):
        m = manager()
        for proj in ("/old-1", "/old-2", "/old-3"):
            self.query(m, proj)
        self.assertIsNone(m.stop_reason)

    def test_but_their_jobs_are_still_failed_so_the_branches_come_back(self):
        """This is the recovery: law resubmits them into a task built with the corrected list."""
        m = manager()
        result, ids = self.query(m, "/old-1")
        self.assertEqual(set(result), set(ids))
        self.assertTrue(all(d["status"] == m.FAILED for d in result.values()))

    def test_the_report_says_it_was_not_this_run_that_sent_it(self):
        m = manager()
        with mock.patch("builtins.print") as printed:
            self.query(m, "/old-1")
        said = "\n".join(str(c.args[0]) for c in printed.call_args_list if c.args)
        self.assertIn("earlier run", said)

    def test_a_refusal_of_this_runs_own_submission_still_counts(self):
        m = manager()
        self.query(m, "/old-1")
        self.query(m, "/old-2")
        m._submitted_projects.update({"/new-1", "/new-2"})
        self.query(m, "/new-1")
        self.assertIsNone(m.stop_reason)
        self.query(m, "/new-2")
        self.assertIn("made by this run", m.stop_reason)

    def test_submitting_is_what_marks_a_task_as_this_runs(self):
        m = manager()
        job_id = m.JobId(1, "task", "/fresh")
        with mock.patch.object(law.cms.CrabJobManager, "submit", return_value=[job_id]):
            m.submit("job.jdl")
        self.assertIn("/fresh", m._submitted_projects)


class AnUnscheduledTaskCannotStallForever(unittest.TestCase):
    """Accepting WAITING must not remove the guard against a task that never leaves it."""

    def query(self, m, proj_dir="/proj"):
        ids = job_ids(m)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            side_effect=lambda *a, **k: DSProdCrabJobManager.parse_query_output(
                WAITING_OUTPUT, proj_dir, ids
            ),
        ), mock.patch("dsprod.crab.time.sleep"):
            return m.query(proj_dir, job_ids=ids)

    def test_a_long_wait_is_repeated_in_the_log_rather_than_said_once(self):
        m = manager(unscheduled_report_every=3)
        with mock.patch("builtins.print") as printed:
            for _ in range(7):
                self.query(m)
        said = [str(c.args[0]) for c in printed.call_args_list if c.args]
        self.assertEqual(len([x for x in said if "WAITING" in x]), 3)
        self.assertIn("polls", said[-1])

    def test_a_task_that_never_reaches_a_scheduler_stops_the_run(self):
        m = manager(max_unscheduled_polls=4)
        for _ in range(4):
            self.query(m)
        self.assertIsNone(m.stop_reason)
        self.query(m)
        self.assertIn("WAITING", m.stop_reason)
        self.assertIn("TaskWorker", m.stop_reason)

    def test_a_task_that_gets_scheduled_forgets_the_wait(self):
        m = manager(max_unscheduled_polls=4)
        for _ in range(3):
            self.query(m)
        ids = job_ids(m)
        with mock.patch.object(
            law.cms.CrabJobManager,
            "query",
            return_value={
                i: m.job_status_dict(job_id=i, status=m.RUNNING) for i in ids
            },
        ):
            m.query("/proj", job_ids=ids)
        self.assertNotIn("/proj", m._unscheduled)


class AFreshlySubmittedTaskIsLawsOwnBusiness(unittest.TestCase):
    """`SUBMITTED` with no job data yet is accepted by law itself, and must stay that way."""

    def test_law_reports_its_jobs_pending_without_dsprod_intervening(self):
        result = DSProdCrabJobManager.parse_query_output(
            SUBMITTED_OUTPUT, "/proj", [1, 2]
        )
        self.assertEqual(
            [d["status"] for d in result.values()],
            [DSProdCrabJobManager.PENDING] * 2,
        )


#: a list long enough to pass the shape guard, as a real one is
MANY_SITES = [f"T2_XX_Site{i:03d}" for i in range(60)]


class TheSiteListCrabValidatesAgainst(unittest.TestCase):
    PAYLOAD = {
        "desc": {"columns": ["type", "site_name", "alias"]},
        "result": [
            ["psn", "T2_DE_DESY", "T2_DE_DESY"],
            ["psn", "T1_US_FNAL", "T1_US_FNAL"],
            ["phedex", "T1_US_FNAL_Disk", "T1_US_FNAL_Disk"],
            ["psn", "T2_CH_CERN", "T2_CH_CERN"],
            ["phedex", "T3_CH_CERN_HelixNebula_REHA", "T3_CH_CERN_HelixNebula_REHA"],
        ],
    }

    def test_only_processing_site_names_survive(self):
        sites = _parse_cric_sites(self.PAYLOAD)
        self.assertEqual(sites, ["T1_US_FNAL", "T2_CH_CERN", "T2_DE_DESY"])
        self.assertNotIn("T3_CH_CERN_HelixNebula_REHA", sites)
        self.assertNotIn("T1_US_FNAL_Disk", sites)

    def test_the_columns_are_read_by_name_not_by_position(self):
        reordered = {
            "desc": {"columns": ["alias", "type", "site_name"]},
            "result": [[row[2], row[0], row[1]] for row in self.PAYLOAD["result"]],
        }
        self.assertEqual(_parse_cric_sites(reordered), _parse_cric_sites(self.PAYLOAD))

    def test_a_payload_of_another_shape_yields_nothing_rather_than_guessing(self):
        for payload in ({}, {"result": None}, {"desc": {}, "result": []}, None):
            self.assertEqual(_parse_cric_sites(payload), [])

    def test_a_short_parse_is_a_failure_not_a_small_site_pool(self):
        """The silent failure this closes: a shrunken whitelist looks like a working production."""
        with mock.patch("dsprod.crab.urllib.request.urlopen"), mock.patch(
            "dsprod.crab.json.load", return_value=self.PAYLOAD
        ), self.assertRaises(RuntimeError) as caught:
            processing_sites(cache_path=None)
        self.assertIn("processing sites", str(caught.exception))

    def test_a_short_cache_is_refused_too_not_only_a_short_fetch(self):
        """Every path out is checked: a truncated cache shrinks the pool just as quietly."""
        with tempfile.TemporaryDirectory() as d:
            cache = os.path.join(d, "cms_psn_sites.json")
            with open(cache, "w") as f:
                json.dump(["T2_DE_DESY"], f)
            with mock.patch(
                "dsprod.crab.urllib.request.urlopen", side_effect=OSError("no network")
            ), self.assertRaises(RuntimeError):
                processing_sites(cache_path=cache)

    def test_an_unreachable_cric_falls_back_to_the_cache_and_says_so(self):
        with tempfile.TemporaryDirectory() as d:
            cache = os.path.join(d, "cms_psn_sites.json")
            with open(cache, "w") as f:
                json.dump(MANY_SITES, f)
            os.utime(cache, (0, 0))  # far older than the 24 h reuse window
            with mock.patch(
                "dsprod.crab.urllib.request.urlopen", side_effect=OSError("no network")
            ), mock.patch("builtins.print") as printed:
                sites = processing_sites(cache_path=cache)
            self.assertEqual(sites, MANY_SITES)
            said = "\n".join(str(c.args[0]) for c in printed.call_args_list if c.args)
            self.assertIn("falling back", said)
            self.assertIn("h ago", said)

    def test_an_unreachable_cric_with_no_cache_raises(self):
        with mock.patch(
            "dsprod.crab.urllib.request.urlopen", side_effect=OSError("no network")
        ), self.assertRaises(RuntimeError) as caught:
            processing_sites(cache_path=None)
        self.assertIn("could not read the CMS site list", str(caught.exception))

    def test_the_cache_is_not_the_file_the_old_rule_wrote(self):
        """A list built by the old rule must not be inherited: it is what got a task refused."""
        path = CrabWorkflow.site_cache_path(
            mock.Mock(ana_data_path=mock.Mock(return_value="/data"))
        )
        self.assertEqual(os.path.basename(path), "cms_psn_sites.json")
        self.assertNotIn("cms_sites.json", path)


if __name__ == "__main__":
    unittest.main()
