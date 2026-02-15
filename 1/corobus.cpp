#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <deque>
#include <vector>

/// Queue that do not need reallocation
class StableQueue {
public:
    explicit StableQueue() {}

	void init(size_t capacity) {
		buffer_.resize(capacity);
		capacity_ = capacity;
	}

    void push(unsigned value) {
        size_t tail = (head_ + size_) % capacity_;
        buffer_[tail] = value;
        ++size_;
    }

    unsigned pop() {
        unsigned val = buffer_[head_];
        head_ = (head_ + 1) % capacity_;
        --size_;
        return val;
    }

	bool full() {
		return size_ == capacity_;
	}

	bool empty() {
		return size_ == 0;
	}

	size_t count_free_space() {
		return capacity_ - size_;
	}

    size_t size() const { return size_; }

private:
    std::vector<unsigned> buffer_;
    size_t capacity_{0};
    size_t head_{0};
    size_t size_{0};
};

typedef struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
} wakeup_entry;

typedef struct wakeup_queue {
	struct rlist coros;
} wakeup_queue;

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	wakeup_entry *entry;
	rlist_foreach_entry(entry, &queue->coros, base) {
		coro_wakeup(entry->coro);
	}
}

typedef struct coro_bus_channel {
	size_t size_limit;
	struct wakeup_queue send_queue;
	struct wakeup_queue recv_queue;
	StableQueue data;
} coro_bus_channel;

struct coro_bus {
	std::vector<coro_bus_channel *> channels;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
    return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
    global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
	return new coro_bus{};
}

void
coro_bus_delete(struct coro_bus *bus)
{
	for (size_t i{0}; i < bus->channels.size(); ++i) {
		if (bus->channels[i] != nullptr) {
			delete bus->channels[i];
		}
	}
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	coro_bus_channel *ch = new coro_bus_channel{};
	ch->data.init(size_limit);
	rlist_create(&ch->send_queue.coros);
	rlist_create(&ch->recv_queue.coros);

	for (size_t i{0}; i < bus->channels.size(); ++i) {
		if (bus->channels[i] == nullptr) {
			bus->channels[i] = ch;
			return (int)i;
		}
	}
	
	bus->channels.push_back(ch);
	
	return (int)(bus->channels.size() - 1);
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (channel < 0 ||
		(size_t)channel >= bus->channels.size() ||
		bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return;
	}

	coro_bus_channel *ch = bus->channels[channel];

	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);

	bus->channels[channel] = nullptr;
	delete ch;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	while (true) {
		int send_res = coro_bus_try_send(bus, channel, data);
		if (send_res == 0) {
			coro_bus_channel *ch = bus->channels[channel];
			if (!(ch->data.full())) {
				wakeup_queue_wakeup_first(&bus->channels[channel]->send_queue);
			}
			return 0;
		}
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		wakeup_queue_suspend_this(&bus->channels[channel]->send_queue);
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size()
		|| bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel *ch = bus->channels[channel];

	if (ch->data.full()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	ch->data.push(data);
	wakeup_queue_wakeup_first(&ch->recv_queue);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		int recv_res = coro_bus_try_recv(bus, channel, data);
		if (recv_res == 0) {
			coro_bus_channel *ch = bus->channels[channel];
			if (ch->data.size() != 0) {
				wakeup_queue_wakeup_first(&bus->channels[channel]->recv_queue);
			}
			return 0;
		}
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;

		wakeup_queue_suspend_this(&bus->channels[channel]->recv_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size()
		|| bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel *ch = bus->channels[channel];

	if (ch->data.size() == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	*data = ch->data.pop();

	wakeup_queue_wakeup_first(&ch->send_queue);
	return 0;
}

#if NEED_BROADCAST

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	bool has_channel = false;
	for (size_t i{0}; i < bus->channels.size(); ++i) {
		if (bus->channels[i] != nullptr) {
			has_channel = true;
			break;
		}
	}
	if (!has_channel) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	for (size_t i{0}; i < bus->channels.size(); ++i) {
		if (bus->channels[i] != nullptr && bus->channels[i]->data.full()) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}

	for (size_t i{0}; i < bus->channels.size(); ++i) {
		if (bus->channels[i] != nullptr) {
			bus->channels[i]->data.push(data);
			wakeup_queue_wakeup_first(&bus->channels[i]->recv_queue);
		}
	}

	return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int res = coro_bus_try_broadcast(bus, data);
		if (res == 0) {
			return 0;
		}
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}
		struct coro *self = coro_this();

		int full_count = 0;
		for (size_t i{0}; i < bus->channels.size(); ++i) {
			if (bus->channels[i] != nullptr && bus->channels[i]->data.full())
				full_count++;
		}

		wakeup_entry *entries = new wakeup_entry[full_count];
		int idx = 0;
		for (size_t i{0}; i < bus->channels.size(); ++i) {
			if (bus->channels[i] != nullptr && bus->channels[i]->data.full()) {
				entries[idx].coro = self;
				rlist_add_tail_entry(
					&bus->channels[i]->send_queue.coros,
					&entries[idx], base);
				idx++;
			}
		}
		coro_suspend();

		for (int i = 0; i < full_count; ++i) {
			rlist_del_entry(&entries[i], base);
		}
		delete[] entries;
	}
}

#endif

#if NEED_BATCH

int
coro_bus_try_send_v(struct coro_bus *bus, int channel,
	const unsigned *data, unsigned count)
{
	if (channel < 0 || channel >= (int)bus->channels.size() ||
		bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->data.full()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	size_t free_space = ch->data.count_free_space();
	size_t to_send = count;
	if (to_send > free_space) {
		to_send = (unsigned)free_space;
	}
	for (unsigned i = 0; i < to_send; ++i) {
		ch->data.push(data[i]);
	}

	wakeup_queue_wakeup_first(&ch->recv_queue);
	return (int)to_send;
}

int
coro_bus_send_v(struct coro_bus *bus, int channel,
	const unsigned *data, unsigned count)
{
	while (true) {
		int send_res = coro_bus_try_send_v(bus, channel, data, count);
		if (send_res > 0) {
			if (channel < (int)bus->channels.size() &&
			    bus->channels[channel] != nullptr &&
			    !bus->channels[channel]->data.full()) {
				wakeup_queue_wakeup_first(&bus->channels[channel]->send_queue);
			}
			return send_res;
		}
		if (send_res < 0 && coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		wakeup_queue_suspend_this(&bus->channels[channel]->send_queue);
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel,
	unsigned *data, unsigned capacity)
{
	if (channel < 0 || channel >= (int)bus->channels.size() ||
	    bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	struct coro_bus_channel *ch = bus->channels[channel];
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	unsigned to_recv = capacity;
	if (to_recv > ch->data.size()) {
		to_recv = (unsigned)ch->data.size();
	}
	for (unsigned i = 0; i < to_recv; ++i) {
		data[i] = ch->data.pop();
	}

	wakeup_queue_wakeup_first(&ch->send_queue);
	return (int)to_recv;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel,
	unsigned *data, unsigned capacity)
{
	while (true) {
		int recv_res = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (recv_res > 0) {
			if (channel < (int)bus->channels.size() &&
			    bus->channels[channel] != nullptr &&
			    !bus->channels[channel]->data.empty()) {
				wakeup_queue_wakeup_first(&bus->channels[channel]->recv_queue);
			}
			return recv_res;
		}
		if (recv_res < 0 && coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK) {
			return -1;
		}

		wakeup_queue_suspend_this(&bus->channels[channel]->recv_queue);
	}
}

#endif
