/*
 * simple.c - a simple scheduler policy
 */

#include <stdlib.h>
#include <string.h>

#include <base/stddef.h>

#include "defs.h"
#include "sched.h"

/* a list of processes that are waiting for more cores */
static LIST_HEAD(congested_procs);
/* a list of processes that are using more than their guaranteed cores */
static DEFINE_BITMAP(simple_idle_cores, NCPU);

struct simple_data {
	struct proc		*p;
	unsigned int		is_congested:1;
	struct list_node	congested_link;

	/* thread usage limits */
	int			threads_guaranteed;
	int			threads_max;
	int			threads_active;

	/* congestion info */
	float			load;
	uint64_t		standing_queue_us;
};

static bool simple_proc_is_preemptible(struct simple_data *cursd,
				       struct simple_data *nextsd)
{
	return cursd->threads_active > cursd->threads_guaranteed &&
	       nextsd->threads_active < nextsd->threads_guaranteed;
}

/* the current process running on each core */
static struct simple_data *cores[NCPU];

/* the history of processes running on each core */
#define NHIST	4
static struct simple_data *hist[NCPU][NHIST];

static void simple_cleanup_core(unsigned int core)
{
	struct simple_data *sd = cores[core];
	int i;

	if (!sd)
		return;

	if (cores[core])
		cores[core]->threads_active--;
	cores[core] = NULL;
	for (i = 1; i < NHIST; i++)
		hist[core][i] = hist[core][i - 1];
	hist[core][0] = sd;
}

static void simple_mark_congested(struct simple_data *sd)
{
	if (sd->is_congested)
		return;
	sd->is_congested = true;
	list_add(&congested_procs, &sd->congested_link);
}

static void simple_unmark_congested(struct simple_data *sd)
{
	if (!sd->is_congested)
		return;
	sd->is_congested = false;
	list_del_from(&congested_procs, &sd->congested_link);
}

static int simple_attach(struct proc *p, struct sched_spec *cfg)
{
	struct simple_data *sd;

	/* TODO: validate if there are enough cores available for @cfg */

	sd = malloc(sizeof(*sd));
	if (!sd)
		return -ENOMEM;

	memset(sd, 0, sizeof(*sd));
	sd->p = p;
	sd->threads_guaranteed = cfg->guaranteed_cores;
	sd->threads_max = cfg->max_cores;
	sd->threads_active = 0;
	p->policy_data = (unsigned long)sd;
	return 0;
}

static void simple_detach(struct proc *p)
{
	struct simple_data *sd = (struct simple_data *)p->policy_data;
	int i, j;

	simple_unmark_congested(sd);

	for (i = 0; i < NCPU; i++) {
		if (cores[i] == sd)
			cores[i] = NULL;
		for (j = 0; j < NHIST; j++) {
			if (hist[i][j] == sd)
				hist[i][j] = NULL;
		}
	}

	free(sd);
}

static int simple_run_kthread_on_core(struct proc *p, unsigned int core)
{
	struct simple_data *sd = (struct simple_data *)p->policy_data;
	int ret;

	/*
	 * WARNING: A kthread could be stuck waiting to detach and thus
	 * temporarily unavailable even if it is no longer assigned to a core.
	 * We check with the scheduler layer here to catch such a race
	 * condition.  In this sense, applications can get new cores more
	 * quickly if they yield promptly when requested.
	 */
	if (sched_threads_avail(p) == 0)
		return -EBUSY;

	ret = sched_run_on_core(p, core);
	if (ret)
		return ret;

	simple_cleanup_core(core);
	cores[core] = sd;
	bitmap_clear(simple_idle_cores, core);
	sd->threads_active++;
	return 0;
}

static unsigned int simple_choose_core(struct proc *p)
{
	struct simple_data *sd = (struct simple_data *)p->policy_data;
	struct thread *th;
	unsigned int core;

	/* first try to find a previously used core (to improve locality) */
	list_for_each(&p->idle_threads, th, idle_link) {
		core = th->core;
		if (core >= NCPU)
			break;
		if (cores[core] != sd && (cores[core] == NULL ||
		    simple_proc_is_preemptible(cores[core], sd))) {
			return core;
		}

		/* sibling core has equally good locality */
		core = sched_siblings[th->core];
		if (cores[core] != sd && (cores[core] == NULL ||
		    simple_proc_is_preemptible(cores[core], sd))) {
			if (bitmap_test(sched_allowed_cores, core))
				return core;
		}
	}

	/* then look for any idle core */
	core = bitmap_find_next_set(simple_idle_cores, NCPU, 0);
	if (core != NCPU)
		return core;

	/* finally look for any preemptible core */
	for (core = 0; core < NCPU; core++) {
		if (cores[core] == sd)
			continue;
		if (cores[core] &&
		    simple_proc_is_preemptible(cores[core], sd))
			return core;
	}

	/* out of luck, couldn't find anything */
	return NCPU;
}

static int simple_add_kthread(struct proc *p)
{
	struct simple_data *sd = (struct simple_data *)p->policy_data;
	unsigned int core;

	if (sd->threads_active >= sd->threads_max)
		return -ENOENT;

	core = simple_choose_core(p);
	if (core == NCPU)
		return -ENOENT;

	return simple_run_kthread_on_core(p, core);
}

static int simple_notify_core_needed(struct proc *p)
{
	return simple_add_kthread(p); 
}

#define EWMA_WEIGHT	0.1f

static void simple_update_congestion_info(struct simple_data *sd)
{
	struct congestion_info *info = sd->p->congestion_info;
	float instant_load;

	/* update the standing queue congestion microseconds */
	if (sd->is_congested)
		sd->standing_queue_us += IOKERNEL_POLL_INTERVAL;
	else
		sd->standing_queue_us = 0;
	ACCESS_ONCE(info->standing_queue_us) = sd->standing_queue_us;

	/* update the CPU load */
	/* TODO: handle using more than guaranteed cores */
	instant_load = (float)sd->threads_active / (float)sd->threads_max;
	sd->load = sd->load * (1 - EWMA_WEIGHT) + instant_load * EWMA_WEIGHT;
	ACCESS_ONCE(info->load) = sd->load;
}

static void simple_notify_congested(struct proc *p, bitmap_ptr_t threads,
				    bitmap_ptr_t io)
{
	struct simple_data *sd = (struct simple_data *)p->policy_data;
	int ret;

	/* check if congested */
	if (bitmap_popcount(threads, NCPU) +
            bitmap_popcount(io, NCPU) == 0) {
		simple_unmark_congested(sd);
		goto done;
	}

	/* do nothing if already marked as congested */
	if (sd->is_congested)
		goto done;

	/* try to add an additional core right away */
	ret = simple_add_kthread(p);
	if (ret == 0)
		goto done;

	/* otherwise mark the process as congested, cores can be added later */
	simple_mark_congested(sd);

done:
	simple_update_congestion_info(sd);
}

static struct simple_data *simple_choose_kthread(unsigned int core)
{
	struct simple_data *sd;
	int i;

	/* first try to run the same process as the sibling */
	sd = cores[sched_siblings[core]];
	if (sd && sd->is_congested)
		return sd;

	/* then try to find a congested process that ran on this core last */
	for (i = 0; i < NHIST; i++) {
		sd = hist[core][i];
		if (sd && sd->is_congested)
			return sd;

		/* the hyperthread sibling has equally good locality */
		sd = hist[sched_siblings[core]][i];
		if (sd && sd->is_congested)
			return sd;
	}

	/* then try to find any congested process */
	return list_top(&congested_procs, struct simple_data, congested_link);
}

static void simple_sched_poll(bitmap_ptr_t idle)
{
	struct simple_data *sd;
	unsigned int core;

	bitmap_for_each_set(idle, NCPU, core) {
		simple_cleanup_core(core);
		sd = simple_choose_kthread(core);
		if (!sd) {
			bitmap_set(simple_idle_cores, core);
			continue;
		}
		simple_unmark_congested(sd);
		if (unlikely(simple_run_kthread_on_core(sd->p, core)))
			bitmap_set(simple_idle_cores, core);
	}
}

struct sched_ops simple_ops = {
	.proc_attach		= simple_attach,
	.proc_detach		= simple_detach,
	.notify_congested	= simple_notify_congested,
	.notify_core_needed	= simple_notify_core_needed,
	.sched_poll		= simple_sched_poll,
};

/**
 * simple_init - initializes the simple scheduler policy
 *
 * Returns 0 (always successful).
 */
int simple_init(void)
{
	bitmap_or(simple_idle_cores, simple_idle_cores,
		  sched_allowed_cores, NCPU);
	return 0;
}
