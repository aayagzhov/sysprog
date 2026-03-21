#include "thread_pool.h"

#include <pthread.h>
#include <vector>
#include <deque>
#include <ctime>
#include <cerrno>

enum task_state {
	TASK_NOT_PUSHED = 0,
	TASK_QUEUED,
	TASK_RUNNING,
	TASK_FINISHED,
};

struct thread_pool;

struct thread_task {
	thread_task_f function;

	mutable pthread_mutex_t mutex;
	pthread_cond_t          cond;

	task_state         state;
	bool               detached;
	struct thread_pool *pool;
};

struct thread_pool {
	pthread_mutex_t  mutex;
	pthread_cond_t   task_available_cond;
	pthread_cond_t   task_done_cond;

	std::deque<thread_task *> queue;
	std::vector<pthread_t>    threads;

	int  max_threads;
	int  active_tasks;
	bool shutdown;
};

static void *
worker_thread(void *arg)
{
	struct thread_pool *pool = static_cast<struct thread_pool *>(arg);

	for (;;) {
		pthread_mutex_lock(&pool->mutex);

		while (pool->queue.empty() && !pool->shutdown)
			pthread_cond_wait(&pool->task_available_cond, &pool->mutex);

		if (pool->shutdown && pool->queue.empty()) {
			pthread_mutex_unlock(&pool->mutex);
			break;
		}

		struct thread_task *task = pool->queue.front();
		pool->queue.pop_front();
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->state = TASK_RUNNING;
		pthread_mutex_unlock(&task->mutex);

		task->function();

		pthread_mutex_lock(&task->mutex);
		task->state = TASK_FINISHED;
		bool should_delete = task->detached;
		pthread_cond_broadcast(&task->cond);
		pthread_mutex_unlock(&task->mutex);

		if (should_delete) {
			pthread_mutex_lock(&pool->mutex);
			--pool->active_tasks;
			pthread_cond_broadcast(&pool->task_done_cond);
			pthread_mutex_unlock(&pool->mutex);

			pthread_mutex_destroy(&task->mutex);
			pthread_cond_destroy(&task->cond);
			delete task;
		}
	}

	return nullptr;
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;

	struct thread_pool *p = new thread_pool();
	pthread_mutex_init(&p->mutex, nullptr);
	pthread_cond_init(&p->task_available_cond, nullptr);
	pthread_cond_init(&p->task_done_cond, nullptr);
	p->max_threads  = thread_count;
	p->active_tasks = 0;
	p->shutdown     = false;

	*pool = p;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);

	if (pool->active_tasks > 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}

	pool->shutdown = true;
	pthread_cond_broadcast(&pool->task_available_cond);
	pthread_mutex_unlock(&pool->mutex);

	for (pthread_t &tid : pool->threads)
		pthread_join(tid, nullptr);

	pthread_mutex_destroy(&pool->mutex);
	pthread_cond_destroy(&pool->task_available_cond);
	pthread_cond_destroy(&pool->task_done_cond);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->mutex);

	if (pool->active_tasks >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	task->state    = TASK_QUEUED;
	task->detached = false;
	task->pool     = pool;
	pthread_mutex_unlock(&task->mutex);

	pool->queue.push_back(task);
	++pool->active_tasks;

	if ((int)pool->threads.size() < pool->max_threads) {
		pthread_t tid;
		pthread_create(&tid, nullptr, worker_thread, pool);
		pool->threads.push_back(tid);
	}

	pthread_cond_signal(&pool->task_available_cond);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	struct thread_task *t = new thread_task();
	t->function = function;
	pthread_mutex_init(&t->mutex, nullptr);
	pthread_cond_init(&t->cond, nullptr);
	t->state    = TASK_NOT_PUSHED;
	t->detached = false;
	t->pool     = nullptr;
	*task = t;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	bool finished = (task->state == TASK_FINISHED);
	pthread_mutex_unlock(&task->mutex);
	return finished;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	bool running = (task->state == TASK_RUNNING);
	pthread_mutex_unlock(&task->mutex);
	return running;
}

int
thread_task_join(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);

	if (task->state == TASK_NOT_PUSHED) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	while (task->state != TASK_FINISHED)
		pthread_cond_wait(&task->cond, &task->mutex);

	struct thread_pool *pool = task->pool;
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	--pool->active_tasks;
	pthread_cond_broadcast(&pool->task_done_cond);
	pthread_mutex_unlock(&pool->mutex);

	pthread_mutex_lock(&task->mutex);
	task->state = TASK_NOT_PUSHED;
	task->pool  = nullptr;
	pthread_mutex_unlock(&task->mutex);

	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	pthread_mutex_lock(&task->mutex);

	if (task->state == TASK_NOT_PUSHED) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (task->state == TASK_FINISHED) {
		struct thread_pool *pool = task->pool;
		pthread_mutex_unlock(&task->mutex);

		pthread_mutex_lock(&pool->mutex);
		--pool->active_tasks;
		pthread_cond_broadcast(&pool->task_done_cond);
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->state = TASK_NOT_PUSHED;
		task->pool  = nullptr;
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}

	if (timeout <= 0) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TIMEOUT;
	}

	struct timespec deadline;
	clock_gettime(CLOCK_REALTIME, &deadline);

	long long ns = (long long)(timeout * 1e9);
	deadline.tv_sec  += ns / 1000000000LL;
	deadline.tv_nsec += ns % 1000000000LL;
	if (deadline.tv_nsec >= 1000000000L) {
		deadline.tv_sec  += 1;
		deadline.tv_nsec -= 1000000000L;
	}

	while (task->state != TASK_FINISHED) {
		int rc = pthread_cond_timedwait(&task->cond, &task->mutex, &deadline);
		if (rc == ETIMEDOUT) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}

	struct thread_pool *pool = task->pool;
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	--pool->active_tasks;
	pthread_cond_broadcast(&pool->task_done_cond);
	pthread_mutex_unlock(&pool->mutex);

	pthread_mutex_lock(&task->mutex);
	task->state = TASK_NOT_PUSHED;
	task->pool  = nullptr;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);

	if (task->state == TASK_QUEUED || task->state == TASK_RUNNING) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}

	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_destroy(&task->mutex);
	pthread_cond_destroy(&task->cond);
	delete task;
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);

	if (task->state == TASK_NOT_PUSHED) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (task->state == TASK_FINISHED) {
		struct thread_pool *pool = task->pool;
		pthread_mutex_unlock(&task->mutex);

		pthread_mutex_lock(&pool->mutex);
		--pool->active_tasks;
		pthread_cond_broadcast(&pool->task_done_cond);
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_destroy(&task->mutex);
		pthread_cond_destroy(&task->cond);
		delete task;
		return 0;
	}

	task->detached = true;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif
